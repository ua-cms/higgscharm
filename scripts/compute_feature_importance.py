#!/usr/bin/env python3
"""
Compute feature importance for trained neural network models.

Implements multiple feature importance methods:
1. Permutation Importance - Model-agnostic, measures accuracy drop when shuffling features
2. Gradient-based Importance - Computes mean absolute gradient of output w.r.t. inputs
3. First-layer Weight Importance - Analyzes weight magnitudes of the first layer

Works with any config and model in the b-hive framework.

Usage:
    python scripts/compute_feature_importance.py \
        --config <config_name> \
        --dataset-version <dataset_version> \
        --model-name <model_name> \
        --training-version <training_version> \
        --epochs <epochs> \
        --output-dir outputs/feature_importance \
        --method all \
        --max-events 10000
"""

import argparse
import os
import sys
import lz4.frame
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from glob import glob
from tqdm import tqdm
import torch
import torch.nn as nn

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config.config_loader import ConfigLoader
from utils.models.models import BTaggingModels


# Standard feature category keys used in configs
FEATURE_CATEGORIES = [
    ('global_features', None, 1),           # (key, n_key, default_n)
    ('cpf_candidates', 'n_cpf_candidates', 1),
    ('npf_candidates', 'n_npf_candidates', 1),
    ('vtx_features', 'n_vtx_candidates', 1),
    ('lt_candidates', 'n_lt_candidates', 1),
]


def get_feature_info(config):
    """
    Get feature information from config.

    Returns:
        feature_names: list of feature names in order
        feature_dims: list of (n_candidates, n_features_per_candidate) tuples
        total_features: total number of features
    """
    feature_names = []
    feature_dims = []

    for feat_key, n_key, default_n in FEATURE_CATEGORIES:
        if feat_key not in config or not config[feat_key]:
            feature_dims.append((0, 0))
            continue

        features = config[feat_key]
        n_candidates = config.get(n_key, default_n) if n_key else 1
        n_features = len(features)

        feature_dims.append((n_candidates, n_features))

        # Generate feature names
        if n_candidates == 1 and n_key is None:
            # Global features - no index suffix
            feature_names.extend(features)
        else:
            # Multi-candidate features - add index suffix
            for i in range(n_candidates):
                for feat in features:
                    feature_names.append(f"{feat}_{i}")

    total_features = sum(d[0] * d[1] for d in feature_dims)
    return feature_names, feature_dims, total_features


def create_feature_edges(feature_dims):
    """Create cumulative edges for splitting flattened features."""
    edges = []
    v = 0
    for n_cand, n_feat in feature_dims:
        v += n_cand * n_feat
        edges.append(v)
    return np.array(edges, dtype=int)


def load_lz4_data(file_paths, config, max_events=None):
    """
    Load data from .lz4 files.

    Data format: [features, process(1), truth_labels(N), weight_nominal(1)]
    Total columns = n_features + 1 (process) + n_truths + 1 (weight)

    Also computes and injects 'in_mass_window' feature if configured,
    matching the behavior of LZ4Dataset during training.
    """
    all_features = []
    all_labels = []
    num_truths = len(config.get('truths', []))

    if num_truths == 0:
        raise ValueError("Config must have 'truths' defined")

    # Check if we need to compute in_mass_window feature
    mass_window = config.get("mass_window", None)
    global_features = config.get("global_features", [])
    has_mass_window_feature = (mass_window is not None) and ("in_mass_window" in global_features)

    if has_mass_window_feature:
        # Stored globals = all global features except computed ones
        stored_globals = [f for f in global_features if f != "in_mass_window"]
        n_stored_global = len(stored_globals)
        feature_name = mass_window.get("feature", "m4l")
        mass_window_src_idx = stored_globals.index(feature_name) if feature_name in stored_globals else None
        mass_window_min = mass_window.get("min", -np.inf)
        mass_window_max = mass_window.get("max", np.inf)
        print(f"Will compute 'in_mass_window': {mass_window_min} <= {feature_name} <= {mass_window_max}")
    else:
        n_stored_global = len(global_features)
        mass_window_src_idx = None

    total_events = 0
    for file_path in tqdm(file_paths, desc="Loading files"):
        if max_events and total_events >= max_events:
            break

        with lz4.frame.open(file_path, mode='r') as f:
            output_data = f.read()

        s = np.frombuffer(output_data, dtype='float32').copy()
        total_cols = int(s[1])
        s = s[2:].reshape(-1, total_cols, order="C")

        # Data layout: [features, process(1), truths(N), weight(1)]
        # n_features = total_cols - 1 (process) - num_truths - 1 (weight)
        n_features = total_cols - 2 - num_truths

        features = s[:, :n_features]
        truth_labels = s[:, -(num_truths+1):-1]  # Skip weight at end
        class_indices = np.argmax(truth_labels, axis=1)

        # Compute and inject in_mass_window feature (same as LZ4Dataset)
        if has_mass_window_feature and mass_window_src_idx is not None:
            m4l_vals = features[:, mass_window_src_idx]
            in_window = ((m4l_vals >= mass_window_min) & (m4l_vals <= mass_window_max)).astype(features.dtype)

            # Insert after stored global features (before cpf/npf/vtx/lt)
            features = np.concatenate([
                features[:, :n_stored_global],
                in_window[:, np.newaxis],
                features[:, n_stored_global:]
            ], axis=1)

        if max_events:
            remaining = max_events - total_events
            features = features[:remaining]
            class_indices = class_indices[:remaining]

        all_features.append(features)
        all_labels.append(class_indices)
        total_events += len(features)

    return np.vstack(all_features), np.concatenate(all_labels)


def get_model_input_dim(model):
    """Get the expected input dimension from the model's first linear layer."""
    for name, module in model.named_modules():
        if isinstance(module, nn.Linear):
            return module.in_features
    return None


def load_model(model_path, config, model_name, device='cpu'):
    """Load trained model from checkpoint."""
    model = BTaggingModels(
        model_name,
        config=config,
        for_inference=True
    )

    checkpoint = torch.load(model_path, map_location=device, weights_only=False)

    if 'model_state_dict' in checkpoint:
        model.load_state_dict(checkpoint['model_state_dict'])
    elif 'state_dict' in checkpoint:
        model.load_state_dict(checkpoint['state_dict'])
    else:
        model.load_state_dict(checkpoint)

    model.to(device)
    model.eval()
    return model


def prepare_model_input(features, feature_dims, device='cpu'):
    """
    Convert flat features array to the format expected by the model.
    Returns tuple of tensors: (global, cpf, npf, vtx, lt)

    Handles variable number of feature categories.
    """
    batch_size = features.shape[0]
    n_data_features = features.shape[1]

    edges = create_feature_edges(feature_dims)
    total_config_features = edges[-1] if len(edges) > 0 else 0

    # Split at edges (excluding the last edge which is total)
    split_edges = edges[:-1]

    if n_data_features >= total_config_features:
        splits = np.split(features[:, :total_config_features], split_edges, axis=1)
    else:
        # Adjust edges for smaller data
        adjusted_edges = [min(e, n_data_features) for e in split_edges]
        adjusted_edges = sorted(set(adjusted_edges))
        splits = np.split(features, adjusted_edges, axis=1)

    # Build tensors for each category
    tensors = []
    for i, (n_cand, n_feat) in enumerate(feature_dims):
        if i < len(splits) and splits[i].size > 0:
            data = splits[i]
            expected_size = n_cand * n_feat

            if data.shape[1] == expected_size and n_cand > 0 and n_feat > 0:
                if n_cand == 1:
                    # Global features: (batch, n_feat)
                    tensor = torch.tensor(data, dtype=torch.float32, device=device)
                else:
                    # Multi-candidate: (batch, n_cand, n_feat)
                    tensor = torch.tensor(
                        data.reshape(batch_size, n_cand, n_feat),
                        dtype=torch.float32, device=device
                    )
            else:
                # Dimension mismatch - create zeros
                if n_cand == 1:
                    tensor = torch.zeros((batch_size, n_feat), dtype=torch.float32, device=device)
                else:
                    tensor = torch.zeros((batch_size, n_cand, n_feat), dtype=torch.float32, device=device)
        else:
            # No data for this category
            if n_cand == 1:
                tensor = torch.zeros((batch_size, n_feat), dtype=torch.float32, device=device)
            else:
                tensor = torch.zeros((batch_size, n_cand, n_feat), dtype=torch.float32, device=device)

        tensors.append(tensor)

    return tuple(tensors)


def compute_permutation_importance(model, features, labels, feature_dims, feature_names,
                                   n_repeats=5, device='cpu'):
    """
    Compute permutation importance by shuffling each feature and measuring accuracy drop.
    """
    print("\nComputing permutation importance...")

    # Baseline accuracy
    model_input = prepare_model_input(features, feature_dims, device)
    with torch.no_grad():
        outputs = model(model_input)
        preds = outputs.argmax(dim=1).cpu().numpy()

    baseline_acc = (preds == labels).mean()
    print(f"Baseline accuracy: {baseline_acc:.4f}")

    importance_scores = []
    n_features = features.shape[1]

    for feat_idx in tqdm(range(n_features), desc="Permutation importance"):
        scores = []
        for _ in range(n_repeats):
            # Shuffle this feature
            features_shuffled = features.copy()
            np.random.shuffle(features_shuffled[:, feat_idx])

            # Compute new accuracy
            model_input = prepare_model_input(features_shuffled, feature_dims, device)
            with torch.no_grad():
                outputs = model(model_input)
                preds = outputs.argmax(dim=1).cpu().numpy()

            shuffled_acc = (preds == labels).mean()
            scores.append(baseline_acc - shuffled_acc)

        importance_scores.append(np.mean(scores))

    return np.array(importance_scores)


def compute_gradient_importance(model, features, labels, feature_dims, device='cpu',
                                target_class=None, batch_size=256):
    """
    Compute gradient-based importance by averaging |dOutput/dInput|.
    """
    print("\nComputing gradient-based importance...")

    n_samples = len(features)
    n_features = features.shape[1]
    gradient_sum = np.zeros(n_features)

    # Calculate split sizes
    split_sizes = [d[0] * d[1] for d in feature_dims]
    total_split = sum(split_sizes)

    # Adjust if needed
    if n_features != total_split:
        if n_features > total_split:
            split_sizes.append(n_features - total_split)
        else:
            # Trim last splits
            diff = total_split - n_features
            for i in range(len(split_sizes) - 1, -1, -1):
                if split_sizes[i] >= diff:
                    split_sizes[i] -= diff
                    break
                else:
                    diff -= split_sizes[i]
                    split_sizes[i] = 0

    split_sizes = [s for s in split_sizes if s > 0]

    model.train()

    for start_idx in tqdm(range(0, n_samples, batch_size), desc="Gradient importance"):
        end_idx = min(start_idx + batch_size, n_samples)
        batch_features = features[start_idx:end_idx]
        batch_labels = labels[start_idx:end_idx]
        curr_batch_size = len(batch_features)

        # Create input tensor with gradients
        flat_tensor = torch.tensor(batch_features, dtype=torch.float32,
                                   device=device, requires_grad=True)

        try:
            splits = torch.split(flat_tensor, split_sizes, dim=1)
        except RuntimeError as e:
            print(f"Warning: Split failed: {e}")
            continue

        # Reshape splits to model input format
        tensors = []
        split_idx = 0
        for i, (n_cand, n_feat) in enumerate(feature_dims):
            expected_size = n_cand * n_feat
            if expected_size == 0:
                if n_cand == 1:
                    tensors.append(torch.zeros((curr_batch_size, 0), device=device))
                else:
                    tensors.append(torch.zeros((curr_batch_size, n_cand, 0), device=device))
                continue

            if split_idx < len(splits) and splits[split_idx].shape[1] == expected_size:
                if n_cand == 1:
                    tensors.append(splits[split_idx])
                else:
                    tensors.append(splits[split_idx].reshape(curr_batch_size, n_cand, n_feat))
                split_idx += 1
            else:
                if n_cand == 1:
                    tensors.append(torch.zeros((curr_batch_size, n_feat), device=device))
                else:
                    tensors.append(torch.zeros((curr_batch_size, n_cand, n_feat), device=device))

        model_input = tuple(tensors)

        # Forward pass
        outputs = model(model_input)

        # Select target class output
        if target_class is not None:
            target_outputs = outputs[:, target_class]
        else:
            batch_indices = torch.arange(len(batch_labels), device=device)
            target_outputs = outputs[batch_indices, torch.tensor(batch_labels, device=device)]

        # Compute gradients
        target_outputs.sum().backward()

        if flat_tensor.grad is not None:
            gradient_sum += np.abs(flat_tensor.grad.cpu().numpy()).sum(axis=0)

        model.zero_grad()

    model.eval()
    return gradient_sum / n_samples


def compute_weight_importance(model, feature_names):
    """
    Compute feature importance from first layer weight magnitudes.
    """
    print("\nComputing weight-based importance...")

    # Find the first linear layer
    first_linear = None
    for name, module in model.named_modules():
        if isinstance(module, nn.Linear):
            first_linear = module
            break

    if first_linear is None:
        print("Warning: No linear layer found in model")
        return None

    weights = first_linear.weight.data.cpu().numpy()
    importance = np.abs(weights).sum(axis=0)

    n_features = len(feature_names)
    if len(importance) != n_features:
        print(f"Warning: Weight dim ({len(importance)}) != feature count ({n_features})")
        if len(importance) < n_features:
            importance = np.pad(importance, (0, n_features - len(importance)))
        else:
            importance = importance[:n_features]

    return importance


def plot_feature_importance(importance_scores, feature_names, output_path,
                            title="Feature Importance", top_n=30):
    """Plot feature importance as a horizontal bar chart."""
    sorted_idx = np.argsort(importance_scores)[::-1][:top_n]

    fig, ax = plt.subplots(figsize=(10, max(6, top_n * 0.3)))

    y_pos = np.arange(len(sorted_idx))
    values = importance_scores[sorted_idx]
    names = [feature_names[i] for i in sorted_idx]

    colors = plt.cm.viridis(values / values.max() if values.max() > 0 else values)

    ax.barh(y_pos, values, color=colors)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(names, fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel('Importance Score')
    ax.set_title(title, fontsize=12, fontweight='bold')

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")


def plot_importance_comparison(importance_dict, feature_names, output_path, top_n=20):
    """Plot comparison of different importance methods."""
    normalized = {}
    for method, scores in importance_dict.items():
        if scores is not None:
            max_score = np.max(np.abs(scores))
            normalized[method] = scores / max_score if max_score > 0 else scores

    if not normalized:
        return

    avg_importance = np.zeros(len(feature_names))
    for scores in normalized.values():
        avg_importance += np.abs(scores)
    avg_importance /= len(normalized)

    top_idx = np.argsort(avg_importance)[::-1][:top_n]

    fig, ax = plt.subplots(figsize=(12, max(6, top_n * 0.4)))

    bar_width = 0.8 / len(normalized)
    y_pos = np.arange(len(top_idx))

    for i, (method, scores) in enumerate(normalized.items()):
        offset = (i - len(normalized) / 2 + 0.5) * bar_width
        ax.barh(y_pos + offset, scores[top_idx], bar_width, label=method, alpha=0.8)

    ax.set_yticks(y_pos)
    ax.set_yticklabels([feature_names[i] for i in top_idx], fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel('Normalized Importance')
    ax.set_title('Feature Importance Comparison', fontsize=12, fontweight='bold')
    ax.legend(loc='lower right')

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Compute feature importance for any model/config')
    parser.add_argument('--config', type=str, required=True, help='Config name')
    parser.add_argument('--dataset-version', type=str, required=True, help='Dataset version')
    parser.add_argument('--model-name', type=str, required=True, help='Model name')
    parser.add_argument('--training-version', type=str, required=True, help='Training version')
    parser.add_argument('--epochs', type=int, required=True, help='Number of epochs')
    parser.add_argument('--output-dir', type=str, default='outputs/feature_importance',
                        help='Output directory')
    parser.add_argument('--max-events', type=int, default=10000, help='Max events to use')
    parser.add_argument('--method', type=str, default='all',
                        choices=['all', 'permutation', 'gradient', 'weight'],
                        help='Importance method')
    parser.add_argument('--n-repeats', type=int, default=5,
                        help='Repeats for permutation importance')
    parser.add_argument('--device', type=str, default='cpu', choices=['cpu', 'cuda'],
                        help='Device to use')
    parser.add_argument('--top-n', type=int, default=30, help='Top N features to plot')
    args = parser.parse_args()

    # Load config
    config = ConfigLoader.load_config(args.config)

    # Get feature info from config
    feature_names, feature_dims, total_config_features = get_feature_info(config)
    print(f"Config: {args.config}")
    print(f"Expected features from config: {total_config_features}")

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Find model checkpoint
    model_dir = f"output/TrainingTask/{args.config}/{args.dataset_version}/{args.training_version}/{args.model_name}/epochs_{args.epochs}/nominal"
    model_path = os.path.join(model_dir, "best_model.pt")

    if not os.path.exists(model_path):
        alt_paths = [
            os.path.join(model_dir, f"model_{args.epochs-1}.pt"),
            f"trainings/{args.training_version}/{args.config}/{args.dataset_version}/{args.model_name}/model_epoch_{args.epochs}.pt",
            f"trainings/{args.training_version}/{args.config}/{args.dataset_version}/{args.model_name}/best_model.pt",
        ]
        for alt_path in alt_paths:
            if os.path.exists(alt_path):
                model_path = alt_path
                break
        else:
            print(f"Error: Model not found at {model_path}")
            sys.exit(1)

    print(f"Using model: {model_path}")

    # Find dataset
    dataset_dir = f"output/DatasetConstructorTask/{args.config}/{args.dataset_version}"
    if not os.path.exists(dataset_dir):
        alt_paths = [
            f"datasets/{args.dataset_version}",
            f"output/DatasetConstructorTask/{args.dataset_version}",
        ]
        for alt_path in alt_paths:
            if os.path.exists(alt_path):
                dataset_dir = alt_path
                break
        else:
            print(f"Error: Dataset not found: {dataset_dir}")
            sys.exit(1)

    lz4_files = sorted(glob(os.path.join(dataset_dir, "*.lz4")))
    if not lz4_files:
        print(f"Error: No .lz4 files found in {dataset_dir}")
        sys.exit(1)

    print(f"Found {len(lz4_files)} .lz4 files")

    # Load data
    print(f"\nLoading data (max {args.max_events} events)...")
    features, labels = load_lz4_data(lz4_files, config, max_events=args.max_events)
    print(f"Loaded {len(features)} events with {features.shape[1]} features")

    # Load model
    print(f"\nLoading model...")
    device = args.device
    if device == 'cuda' and not torch.cuda.is_available():
        print("Warning: CUDA not available, using CPU")
        device = 'cpu'

    model = load_model(model_path, config, args.model_name, device)
    model_input_dim = get_model_input_dim(model)
    print(f"Model loaded. Expected input dim: {model_input_dim}")

    # Adjust features if needed
    data_features = features.shape[1]
    if model_input_dim and data_features != model_input_dim:
        if data_features < model_input_dim:
            print(f"Padding data from {data_features} to {model_input_dim}")
            features = np.hstack([features, np.zeros((len(features), model_input_dim - data_features))])
            feature_names.extend([f"pad_{i}" for i in range(data_features, model_input_dim)])
        else:
            print(f"Truncating data from {data_features} to {model_input_dim}")
            features = features[:, :model_input_dim]
            feature_names = feature_names[:model_input_dim]

    # Ensure feature_names matches data
    while len(feature_names) < features.shape[1]:
        feature_names.append(f"feature_{len(feature_names)}")
    feature_names = feature_names[:features.shape[1]]

    print(f"Final: {features.shape[1]} features, {len(feature_names)} names")

    # Compute importance
    importance_dict = {}

    if args.method in ['all', 'permutation']:
        try:
            perm_imp = compute_permutation_importance(
                model, features, labels, feature_dims, feature_names,
                n_repeats=args.n_repeats, device=device
            )
            importance_dict['Permutation'] = perm_imp
            plot_feature_importance(
                perm_imp, feature_names,
                os.path.join(args.output_dir, f"permutation_importance_{args.config}.png"),
                title="Permutation Feature Importance", top_n=args.top_n
            )
        except Exception as e:
            print(f"Warning: Permutation importance failed: {e}")

    if args.method in ['all', 'gradient']:
        try:
            grad_imp = compute_gradient_importance(
                model, features, labels, feature_dims, device=device
            )
            importance_dict['Gradient'] = grad_imp
            plot_feature_importance(
                grad_imp, feature_names,
                os.path.join(args.output_dir, f"gradient_importance_{args.config}.png"),
                title="Gradient-based Feature Importance", top_n=args.top_n
            )
        except Exception as e:
            print(f"Warning: Gradient importance failed: {e}")

    if args.method in ['all', 'weight']:
        try:
            weight_imp = compute_weight_importance(model, feature_names)
            if weight_imp is not None:
                importance_dict['Weight'] = weight_imp
                plot_feature_importance(
                    weight_imp, feature_names,
                    os.path.join(args.output_dir, f"weight_importance_{args.config}.png"),
                    title="First-layer Weight Importance", top_n=args.top_n
                )
        except Exception as e:
            print(f"Warning: Weight importance failed: {e}")

    # Comparison plot
    if len(importance_dict) > 1:
        plot_importance_comparison(
            importance_dict, feature_names,
            os.path.join(args.output_dir, f"importance_comparison_{args.config}.png"),
            top_n=args.top_n
        )

    # Save CSV
    results_df = pd.DataFrame({'feature': feature_names})
    for method, scores in importance_dict.items():
        if scores is not None:
            results_df[method] = scores

    csv_path = os.path.join(args.output_dir, f"feature_importance_{args.config}.csv")
    results_df.to_csv(csv_path, index=False)
    print(f"\nSaved: {csv_path}")

    # Print top features
    print("\n" + "=" * 60)
    print("TOP 20 FEATURES BY IMPORTANCE")
    print("=" * 60)

    for method, scores in importance_dict.items():
        if scores is not None:
            print(f"\n{method} Importance:")
            sorted_idx = np.argsort(scores)[::-1][:20]
            for rank, idx in enumerate(sorted_idx, 1):
                print(f"  {rank:2d}. {feature_names[idx]:30s} {scores[idx]:.6f}")

    print("\nDone!")


if __name__ == "__main__":
    main()
