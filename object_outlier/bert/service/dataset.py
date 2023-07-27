import torch
from torch.utils.data import Dataset

class NerCollator():    
    def __init__(self, tokenizer, max_length, label2id):
        self.tokenizer = tokenizer
        self.label2id = label2id
        self.max_length = max_length

    def __call__(self, input):
        input_texts, input_labels_str = [], []
        offset_mappings = []
        char_labels = []
        
        for input in input:
            text, label_strs = input["dictionary"], input["token_label"]
            input_texts.append(text)
            input_labels_str.append(label_strs)
            offset_mappings.append(input["offset_mapping"])
            char_labels.append(input["char_label"])

        encoded_texts = self.tokenizer.batch_encode_plus(
            input_texts,
            add_special_tokens=True,
            max_length=self.max_length,
            truncation=True,
            padding=True,
            return_tensors="pt",
            return_token_type_ids=True,
            return_attention_mask=True,
            return_offsets_mapping=True
        )
        input_ids = encoded_texts["input_ids"]
        token_type_ids = encoded_texts["token_type_ids"]
        attention_mask = encoded_texts["attention_mask"]

        len_input = input_ids.size(1) 
        input_labels = []
        for input_label_str in input_labels_str:
            input_label = [self.label2id[x] for x in input_label_str]
            if len(input_label) > len_input - 2:
                input_label = input_label[:len_input - 2]
                input_label = [-100] + input_label + [-100]
            else:
                input_label = (
                    [-100] + input_label + (len_input - len(input_label_str) - 1) * [-100]
                )
            input_label = torch.tensor(input_label).long()
            input_labels.append(input_label)

        input_labels = torch.stack(input_labels)
        return input_ids, token_type_ids, attention_mask, input_labels, offset_mappings, char_labels


class NerDataset(Dataset):
    def __init__(self, dataset):
        self.dataset = dataset
    
    def __len__(self):
        return len(self.dataset)
    
    def __getitem__(self, index):
        instance = self.dataset[index]
   
        return instance
