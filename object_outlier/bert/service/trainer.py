from copy import deepcopy
import logging
import numpy as np
import torch
from tqdm import tqdm, trange
from seqeval.metrics import precision_score, recall_score, f1_score, classification_report, accuracy_score
from service.evaluation import token_to_char_label

logger = logging.getLogger(__name__)

class Trainer():
    def __init__(self, model, optimizer, id2label, config):
        self.model = model
        self.optimizer = optimizer
        self.id2label = id2label
        self.config = config
        # self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.device = torch.device("cpu")
        
        super().__init__()
        
    def _train(self, train_loader) :
        self.model.train()
        total_loss = 0.0
        

        tepoch = tqdm(train_loader, unit="batch", position=1, leave=True)
        for batch in tepoch:
            tepoch.set_description(f"Train")
            self.model.zero_grad()
             
            input_ids = batch[0].to(self.device)
            token_type_ids = batch[1].to(self.device)
            attention_mask = batch[2].to(self.device)
            labels = batch[3].to(self.device)

            inputs = {
                "input_ids": input_ids,
                "attention_mask": attention_mask,
                "token_type_ids": token_type_ids,
                "labels": labels,
            }

            outputs = self.model(**inputs)

            loss = outputs[0]
            loss.backward()

            torch.nn.utils.clip_grad_norm_(self.model.parameters(), self.config.max_grad_norm)
            self.optimizer.step()
            total_loss += loss.item()

            tepoch.set_postfix(loss=loss.mean().item())
        tepoch.set_postfix(loss=total_loss / len(train_loader))
        return total_loss / len(train_loader)

    def _validate(self, valid_loader):
        total_loss = 0.0

        self.model.eval()
        all_char_preds = []
        all_char_labels = []
        all_token_predictions = []
        all_token_labels = []

        tepoch = tqdm(valid_loader, unit="batch", leave=False)
        for batch in tepoch:
            tepoch.set_description(f"Valid")
            with torch.no_grad():
                input_ids = batch[0].to(self.device)
                token_type_ids = batch[1].to(self.device)
                attention_mask = batch[2].to(self.device)
                labels = batch[3].to(self.device)
                offset_mappings = batch[4]
                char_labels = batch[5]
                inputs = {
                    "input_ids": input_ids,
                    "token_type_ids": token_type_ids,
                    "attention_mask": attention_mask,
                    "labels": labels,
                }

                outputs = self.model(**inputs)

                loss, logits = outputs[:2]
                total_loss += loss.item()

                token_predictions = logits.argmax(dim=2) 
                token_predictions = token_predictions.detach().cpu().numpy()
                

                char_predictions = token_to_char_label(token_predictions, labels, offset_mappings, self.id2label)
                for j, (char_pred, char_label) in enumerate(zip(char_predictions, char_labels)):
                    if len(char_pred) != len(char_label): # unknown 처리
                        del char_predictions[j]
                        del char_labels[j]

                all_char_preds.extend(char_predictions)
                all_char_labels.extend(char_labels)
            
                for token_prediction, label in zip(token_predictions, labels):
                    filtered = []
                    filtered_label = []
                    for i in range(len(token_prediction)):
                        if label[i].tolist() == -100:
                            continue
                        filtered.append(self.id2label[token_prediction[i]])
                        filtered_label.append(self.id2label[label[i].tolist()])
                    assert len(filtered) == len(filtered_label)
                    all_token_predictions.append(filtered)
                    all_token_labels.append(filtered_label)

            tepoch.set_postfix(loss=loss.mean().item())

        token_f1 = f1_score(all_token_labels, all_token_predictions, average="macro")
        # char_f1 = f1_score(all_char_preds, all_char_labels, average="macro")
        
        return total_loss / len(valid_loader), token_f1

    def train(self, train_loader, valid_loader):
        # model.to("cuda")
        lowest_loss = np.inf
        best_fl = 0.0
        best_model = None
        
        tepoch = trange(self.config.n_epochs, position=0, leave=True)
        for epoch in tepoch:
            tepoch.set_description(f'Epoch {epoch}')
            train_loss = self._train(train_loader)
            valid_loss, token_f1 = self._validate(valid_loader)

            if valid_loss <= lowest_loss:
                lowest_loss = valid_loss
                best_model = deepcopy(self.model.state_dict())
            
            # if best_fl < token_f1:
            #    best_fl = token_f1
            #    best_model = deepcopy(model.state_dict())
                        
            #tepoch.set_postfix(valid_f1 = token_f1 )
            print("Epoch(%d/%d): train_loss=%.4e  valid_loss=%.4e  lowest_loss=%.4e" % (
                 epoch + 1,
                 self.config.n_epochs,
                 train_loss,
                 valid_loss,
                 lowest_loss,
             ))           

            # print("Epoch(%d/%d): train_loss=%.4e  valid_loss=%.4e  lowest_loss=%.4e" % (
            #     epoch_index + 1,
            #     self.config.n_epochs,
            #     train_loss,
            #     valid_loss,
            #     lowest_loss,
            # ))

        self.model.load_state_dict(best_model)


