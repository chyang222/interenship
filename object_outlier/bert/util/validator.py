from cerberus import Validator

def validator(path):
    if path == '/model/train/db':
        return Validator(model_train_db_schema)
    elif path == '/model/train/file':
        return Validator(model_train_file_schema)
    elif path == '/model/inference':
        return Validator(model_inference_schema)
    elif path == '/outlier/object':
        return Validator(outlier_object_schema)

model_train_db_schema = {
    'model' : {'type' : 'string', 'allowed' : ['klue/bert-base', 'kykim/bert-kor-base', 'kykim/albert-kor-base', 'beomi/kcbert-base', 'beomi/kcbert-large']},
    'limit' : {'type' : 'integer'}
}

model_train_file_schema = {
    'model' : {'type' : 'string', 'allowed' : ['klue/bert-base', 'kykim/bert-kor-base', 'kykim/albert-kor-base', 'beomi/kcbert-base', 'beomi/kcbert-large']}
}

model_inference_schema = {
    'data' : {'type' : 'string', 'required' : True}
}

outlier_object_schema = {
    'data' : {'type' : 'string', 'required' : True},
    'column' : {'type' : 'string', 'required' : True},
    'sensitivity' : {'type': 'integer', 'required': True}
}