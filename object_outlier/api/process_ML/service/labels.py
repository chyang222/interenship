import pandas as pd

def labels(labels):
    # String label값을 tensor로 변환하기 위해
    label2id = {label: i for i, label in enumerate(labels)}
    id2label = {i: label for label, i in label2id.items()}
    return label2id, id2label