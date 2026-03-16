def process( fields, arg):
    # Keep only lines that contain 'apple' or 'banana'
    return fields if any(word in fields for word in arg.split(",")) else None