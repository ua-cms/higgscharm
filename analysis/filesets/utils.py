def divide_list(lst: list) -> list:
    """Divide a list into sublists such that each sublist has at least 20 elements."""
    if len(lst) < 20:
        return [lst]

    # Dynamically calculate the number of sublists such that each has at least 20 elements
    n = len(lst) // 20  # This gives the number of groups with at least 20 elements
    if len(lst) % 20 != 0:
        n += 1  # Increase n by 1 if there is a remainder, to accommodate extra elements

    # Divide the list into 'n' sublists
    size = len(lst) // n
    remainder = len(lst) % n
    result = []
    start = 0

    for i in range(n):
        if i < remainder:
            end = start + size + 1
        else:
            end = start + size
        result.append(lst[start:end])
        start = end
    return result
