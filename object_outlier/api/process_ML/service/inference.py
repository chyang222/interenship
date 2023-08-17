from config import bert_model

import argparse
import torch
from transformers import AutoTokenizer
from transformers import BertForTokenClassification, AlbertForTokenClassification
from tqdm import tqdm

def define_argparser():
    p = argparse.ArgumentParser()

    p.add_argument('--model_fn', default = bert_model)
    p.add_argument('--gpu_id', type = int, default = -1)
    p.add_argument('--batch_size', type = int, default = 256)

    config = p.parse_args()

    return config

def main(data):
    config = define_argparser()
    result = list()
    
    saved_data = torch.load(
        config.model_fn,
        map_location = 'cpu' if config.gpu_id < 0 else 'cuda:%d' % config.gpu_id
    )

    train_config = saved_data['config']
    bert_best = saved_data['bert']
    index_to_label = saved_data['classes']
    pretrained_model_name = saved_data['pretrained_model_name']

    with torch.no_grad():  

        tokenizer = AutoTokenizer.from_pretrained(pretrained_model_name)
        model_loader = AlbertForTokenClassification if train_config.use_albert else BertForTokenClassification
        model = model_loader.from_pretrained(
            pretrained_model_name,
            num_labels = len(index_to_label)
        )
        model.load_state_dict(bert_best)

        if config.gpu_id >= 0:
            model.cuda(config.gpu_id)
        # device = next(model.parameters()).device

        model.eval()
        
        tepoch = tqdm(data, unit = "batch")
        for batch in tepoch:
            inputs = tokenizer(
                batch,
                truncation = True,
                add_special_tokens = False,
                padding = True,
                return_tensors = "pt",
            )
            
            # predictions = inputs['input_ids']
            # predictions = predictions.to(device)
            # mask = inputs['attention_mask']
            # mask = mask.to(device)
            
            outputs = model(**inputs)
            
             # 예측 결과에서 가장 큰 값의 인덱스를 구해서 개체명 태그 ID를 얻음
            predictions = torch.argmax(outputs.logits, dim = -1)
            predictions = predictions.detach().cpu().numpy()  
            
            # 토큰화된 문장의 토큰 수 만큼 반복하면서 개체명 태그 출력
            for i, prediction in enumerate(predictions[0]):
                token = tokenizer.convert_ids_to_tokens(inputs.input_ids[0][i].item())
                # 개체명 태그 ID를 개체명 태그로 변환
                label = index_to_label[prediction.item()]
                
                if label == "O":
                   continue
                
                result.append({'token' : token, 'label' : label})
    
    return result
