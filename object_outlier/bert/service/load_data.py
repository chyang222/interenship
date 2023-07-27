import logging
import re
from pathlib import Path

import transformers
from transformers import PreTrainedTokenizer

logger = logging.getLogger(__name__)

def load_data(file_path, tokenizer, max_length):
    data = Path(file_path)
    text = data.read_text(encoding="utf8").strip()
    documents = text.split("\n\n") 
    
    ## dictionary 데이터 가공 과정 추가 해야 함.
    data_list = []
    label_list = list()
    for doc in documents:
        char_labels = [] 
        token_labels = []
        chars = []
        dictionary = ""
        for line in doc.split("\n"):
            if line.startswith("##"):
                continue
            token, tag = line.split("\t")
            dictionary += token
            char_labels.append(tag)
            chars.append(token)
        
        offset_mappings = tokenizer(dictionary, max_length=max_length, return_offsets_mapping=True, truncation=True)["offset_mapping"]
        for offset in offset_mappings:
            start, end = offset
            if start == end == 0:
                continue
            token_labels.append(char_labels[start])

        instance = {
            "dictionary": dictionary,
            "token_label": token_labels,
            "char_label": char_labels,
            "offset_mapping": offset_mappings
        }
        data_list.append(instance)
        
        token_label_list = list(set(token_labels))
        for label in token_label_list:
            label_list.append(label)
        
    label_list = list(set(label_list))

    return data_list, label_list