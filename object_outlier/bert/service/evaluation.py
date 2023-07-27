# Train : Character -> token
# Evaluation : Token -> Character
def token_to_char_label(token_predictions, labels, offset_mapping_batch, id2label):
    char_predictions = []
    for token_predicts, label, offset_mappings in zip(token_predictions, labels, offset_mapping_batch):

        # SPECIAL token 제외
        filtered = []
        for i in range(len(token_predicts)):
            if label[i].tolist() == -100:
                continue
            filtered.append(token_predicts[i])
        char_prediction = []

        # SPECIAL token 제외
        if offset_mappings[0][0] == 0 and offset_mappings[0][1] == 0:
            del offset_mappings[0]
        if offset_mappings[-1][0] == 0 and offset_mappings[-1][1] == 0:
            del offset_mappings[-1]
        assert len(filtered) == len(offset_mappings)

        prev_end = None
        for token_predict, offset_mapping in zip(filtered, offset_mappings):
            start, end = offset_mapping

            # 이전 end와 현재 start가 1개이상 차이나면 띄어쓰기를 추가한다
            if prev_end != None and start - prev_end > 0:
                char_prediction.append("O") # 띄어쓰기
            prev_end = end

            # 싱글 라벨
            if end - start == 1:
                label_str = id2label[token_predict]
                char_prediction.append(label_str)
                continue
            
            # 멀티 라벨
            for i in range(end - start):
                label_str = id2label[token_predict]
                if i == 0 or label_str == "O":
                    char_prediction.append(label_str)
                    continue
                char_prediction.append("I-" + label_str.split("-")[1])
                
        char_predictions.append(char_prediction)
        
    return char_predictions      
