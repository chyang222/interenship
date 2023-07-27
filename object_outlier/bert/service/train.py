import argparse
import random
from pathlib import Path
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, Dataset
from transformers import AutoModel, AutoTokenizer
from transformers import PreTrainedTokenizer, BertConfig, BertForTokenClassification, AlbertForTokenClassification
from transformers import AdamW
import torch_optimizer as custom_optim
from transformers import get_linear_schedule_with_warmup
from service.dataset import NerDataset, NerCollator
from service.trainer import Trainer 
from service.load_data import load_data
from service.labels import labels

from config import bert_model
import os
import shutil
from datetime import datetime

# config
def define_argparser():
    p = argparse.ArgumentParser()

    # p.add_argument('--model_fn', required = True)
    # p.add_argument('--train_fn', required = True)
    
    # model list:
    # - klue/bert-base 
    # - kykim/bert-kor-base
    # - kykim/albert-kor-base
    # - beomi/kcbert-base
    # - beomi/kcbert-large
    # p.add_argument('--pretrained_model_name', type = str, default = 'kykim/bert-kor-base')
    p.add_argument('--use_albert', action = 'store_true') 
    
    p.add_argument('--gpu_id', type = int, default = -1)
    p.add_argument('--verbose', type = int, default = 2)

    p.add_argument('--batch_size', type = int, default = 256)
    p.add_argument('--n_epochs', type = int, default = 100)

    p.add_argument('--lr', type = float, default = 5e-5)
    p.add_argument('--warmup_ratio', type = float, default = .2)
    p.add_argument('--adam_epsilon', type = float, default = 1e-8)
    p.add_argument('--max_grad_norm', type = float, default = 1.0)
    
    # RAdam 사용 시:  LR=1e-4. / warmup_ratio=0.
    p.add_argument('--use_radam', action = 'store_true')
    p.add_argument('--valid_ratio', type = float, default = .2)

    p.add_argument('--max_length', type = int, default = 128)

    config = p.parse_args()

    return config


def get_loaders(fn, tokenizer, valid_ratio=.2):
    config = define_argparser()
    texts, label_list = load_data(fn, tokenizer, config.max_length)

    # 데이터셋 분할 전 셔플 :  DB에서 가져올 때 셔플 후 셔플된 데이터를 load.
    # shuffled = list(zip(texts, labels))
    # random.shuffle(shuffled)
    # texts = [e[0] for e in shuffled]
    # labels = [e[1] for e in shuffled]
    idx = int(len(texts) * (1 - valid_ratio))
    label2id, id2label = labels(label_list)
    
    # collate_fn에 tokenizer를 사용하여 dataloader 가져오기.
    train_loader = DataLoader(
        NerDataset(texts[:idx]),
        batch_size = config.batch_size,
        shuffle=True,
        collate_fn = NerCollator(tokenizer, config.max_length, label2id),
    )
    valid_loader = DataLoader(
        NerDataset(texts[idx:]),
        batch_size = config.batch_size,
        shuffle = False,
        collate_fn = NerCollator(tokenizer, config.max_length, label2id),
    )

    return train_loader, valid_loader, id2label


def get_optimizer(model, config):   
    if config.use_radam:
        optimizer = custom_optim.RAdam(model.parameters(), lr=config.lr)
    else:
        # optimizer / schedule
        no_decay = ['bias', 'LayerNorm.weight']
        optimizer_grouped_parameters = [
            {
                'params': [p for n, p in model.named_parameters() if not any(nd in n for nd in no_decay)],
                'weight_decay': 0.01
            },
            {
                'params': [p for n, p in model.named_parameters() if any(nd in n for nd in no_decay)],
                'weight_decay': 0.0
            }
        ]

        optimizer = AdamW(
            optimizer_grouped_parameters,
            lr=config.lr,
            eps=config.adam_epsilon
        )

    return optimizer


def main(train_fn, pretrained_model_name):
    config = define_argparser()
    
    # pretrained tokenizer.
    tokenizer = AutoTokenizer.from_pretrained(pretrained_model_name)

    train_loader, valid_loader, index_to_label = get_loaders(
        train_fn, 
        tokenizer,
        valid_ratio=config.valid_ratio
    )

    print(
        '|train| =', len(train_loader) * config.batch_size,
        '|valid| =', len(valid_loader) * config.batch_size,
    )

    n_total_iterations = len(train_loader) * config.n_epochs
    n_warmup_steps = int(n_total_iterations * config.warmup_ratio)
    print(
        '#total_iters =', n_total_iterations,
        '#warmup_iters =', n_warmup_steps,
    )

    model_loader = AlbertForTokenClassification if config.use_albert else BertForTokenClassification
    model = model_loader.from_pretrained(
        pretrained_model_name,
        num_labels=len(index_to_label)
    )
    optimizer = get_optimizer(model, config)
    

    # crit = nn.CrossEntropyLoss()
    # scheduler = get_linear_schedule_with_warmup(
    #     optimizer,
    #     n_warmup_steps,
    #     n_total_iterations
    # )

    if config.gpu_id >= 0:
        model.cuda(config.gpu_id)
        # crit.cuda(config.gpu_id)

    # Start train.
    trainer = Trainer(model, optimizer, index_to_label, config)
    trainer.train(
        train_loader,
        valid_loader
    )
    
    # 기존 모델 백업
    if os.path.exists(bert_model):
        bkup_dir_path = os.path.join(os.path.split(bert_model)[0], 'bkup')
        model_name = os.path.split(bert_model)[1]
        
        if not os.path.exists(bkup_dir_path):
            os.makedirs(bkup_dir_path)
        
        model_bkup_path = os.path.join(bkup_dir_path, '{}_{}{}'.format(os.path.splitext(model_name)[0], datetime.now(), os.path.splitext(model_name)[1]))
        shutil.move(bert_model, model_bkup_path)

    torch.save({
        'bert': trainer.model.state_dict(),
        'pretrained_model_name': pretrained_model_name,
        'config': config,
        'classes': index_to_label,
        'tokenizer': tokenizer,
    }, bert_model)

if __name__ == '__main__':
    main()
