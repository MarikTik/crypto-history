from typing import Callable, Coroutine

def binary_search_first_occurrence(
    condition: Callable[[int], bool], 
    start: int, 
    end: int, 
    max_depth: int = 32
) -> int:
    """
    Finds the first occurrence where `condition` is True using binary search.

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

    if max_depth == 0 or start == end:  # Stopping condition
        return start if condition(start) else -1 

    middle = (end + start) // 2

    if condition(middle):  # If condition is met, search left
        return binary_search_first_occurrence(condition, start, middle, max_depth - 1)
    else:  # If condition is not met, search right
        return binary_search_first_occurrence(condition, middle, end, max_depth - 1)
    

async def binary_search_first_occurrence_async(    
    condition: Callable[[int], bool], 
    start: int, 
    end: int, 
    max_depth: int
) -> Coroutine:
    """
    Finds the first occurrence where `condition` is True using binary search.
    This is an asyncronous version of `binary_search_first_occurence` method.
    
    Args:
        condition (Callable[[int], bool]): Function returning True when the target is found.
        start (int): Lower bound of the search range.
        end (int): Upper bound of the search range.
        max_depth (int): Limits recursion depth to control precision.

    Returns:
        Coroutine[int]: 
            The first occurrence where `condition(start)` is True.
            -1 if the condition was never met or `max_depth` was exceeded

    Raises:
        ValueError: If `start > end`, preventing incorrect logic.
    """
    if start > end:
        raise ValueError(f"Invalid range: start ({start}) is after end ({end}).")
    
    if max_depth == 0 or start == end:  # Stopping condition
        return start if await condition(start) else -1  # ✅ FIX: Ensure condition is awaited

    middle = (start + end) // 2  # ✅ FIX: Integer division for correct midpoint

    if await condition(middle):  # ✅ FIX: Ensure condition is awaited
        return await binary_search_first_occurrence_async(condition, start, middle - 1, max_depth - 1)  
    else:
        return await binary_search_first_occurrence_async(condition, middle + 1, end, max_depth - 1)
