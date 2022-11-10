import numpy as np
import pickle

class model:
    def __init__(self,model_path, users_path, items_path):
    #, top_20_path, do_rand=False):
        self.model=pickle.load(open(model_path, 'rb'))
        self.items=np.loadtxt(items_path, dtype=str)
        self.users=set(np.loadtxt(users_path, dtype=int))
        # self.default=list(np.loadtxt(top_20_path, dtype=str))
        # self.do_rand=do_rand

    def get_top_preds(self, user_id:int):
        scores=[self.model.predict(user_id,i).est for i in self.items]
        indices=sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:20]
        top_k=[self.items[i] for i in indices]
        return top_k

    def recommend(self, user_id:int):
        if(user_id in self.users):
            top_k=self.get_top_preds(user_id)
            return top_k
        else:
            return list(np.random.choice(self.items,20))



