def normalize_str(x):
    if x is None:
        return x
    return " ".join(x.replace("\n", " ").split()).lower()
