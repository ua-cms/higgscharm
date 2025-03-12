def dump_lumi(events, output):
    """add run and lumiblock to metadata"""
    import numpy as np
    from coffea import processor
    pairs = np.vstack((events.run.to_numpy(), events.luminosityBlock.to_numpy()))
    # remove replicas
    pairs = np.unique(np.transpose(pairs), axis=0)
    pairs = pairs[
        np.lexsort(([pairs[:, i] for i in range(pairs.shape[1] - 1, -1, -1)]))
    ]
    output["metadata"].update({"run": processor.column_accumulator(pairs[:, 0])})
    output["metadata"].update({"lumi": processor.column_accumulator(pairs[:, 1])})