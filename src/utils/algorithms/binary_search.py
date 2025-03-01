from typing import Callable

async def binary_search_first_occurrence_async(    
    condition: Callable[[int], bool], 
    start: int, 
    end: int, 
    max_depth: int
) -> int:
    """
    Finds the first occurrence where `condition` is True using binary search.
    This is an asyncronous version of `binary_search_first_occurence` method.
    
    Args:
        condition (Callable[[int], bool]): Function returning True when the target is found.
        start (int): Lower bound of the search range.
        end (int): Upper bound of the search range.
        max_depth (int): Limits recursion depth to control precision.

    Returns:
        int: 
            The first occurrence where `condition(start)` is True.
            -1 if the condition was never met or `max_depth` was exceeded

    Raises:
        ValueError: If `start > end`, preventing incorrect logic.
    """
    if start > end:
        raise ValueError(f"Invalid range: start ({start}) is after end ({end}).")
    
    if max_depth == 0:
        return -1  
    
    middle = (start + end) // 2  

    if start == end:
        return start if await condition(start) else -1
    
    if await condition(middle):
        if middle > start and await condition(middle - 1):    # Check if the previous timestamp also has data
            return await binary_search_first_occurrence_async(condition, start, middle - 1, max_depth - 1)
        else:
            return middle  # Found the first occurrence
    else:
        return await binary_search_first_occurrence_async(condition, middle + 1, end, max_depth - 1)