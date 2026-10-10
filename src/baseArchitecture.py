import torch
import torch.nn as nn
from typing import List
from transformers import AutoModel, AutoTokenizer


class DualEncoder(nn.Module):
    def __init__(
        self,
        model_name: str = "bert-base-uncased",
        max_question_len: int = 64,
        max_passage_len: int = 256,
    ):
        super().__init__()
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.question_encoder = AutoModel.from_pretrained(model_name)
        self.passage_encoder = AutoModel.from_pretrained(model_name)
        self.max_question_len = max_question_len
        self.max_passage_len = max_passage_len

    @property
    def device(self):
        return next(self.parameters()).device

    @staticmethod
    def _cls_vector(encoder: nn.Module, batch) -> torch.Tensor:
        """Run BERT and extract the [CLS] hidden state as a vector of shape (B, hidden_size)."""
        out = encoder(**batch)
        return out.last_hidden_state[:, 0, :]

    def encode_questions(self, questions: List[str]) -> torch.Tensor:
        """Encode a list of questions into a vector matrix of shape (B, hidden_size)."""
        batch = self.tokenizer(
            questions,
            padding=True,
            truncation=True,
            max_length=self.max_question_len,
            return_tensors="pt",
        ).to(self.device)
        return self._cls_vector(self.question_encoder, batch)

    def encode_passages(self, titles: List[str], texts: List[str]) -> torch.Tensor:
        """Encode passages as [CLS] title [SEP] text [SEP],
        truncating only the text when necessary."""
        batch = self.tokenizer(
            titles,
            texts,
            padding=True,
            truncation="only_second",
            max_length=self.max_passage_len,
            return_tensors="pt",
        ).to(self.device)
        return self._cls_vector(self.passage_encoder, batch)

    @staticmethod
    def similarity(q_vecs: torch.Tensor, p_vecs: torch.Tensor) -> torch.Tensor:
        """Compute dot-product similarity between question and passage vectors."""
        return q_vecs @ p_vecs.T

    def forward(self, questions: List[str], titles: List[str], texts: List[str]) -> torch.Tensor:
        q = self.encode_questions(questions)
        p = self.encode_passages(titles, texts)
        return self.similarity(q, p)

#test
if __name__ == "__main__":
    model = DualEncoder()
    model.eval()

    questions = [
        "Who is the bad guy in lord of the rings?",
        "What is the body of water between England and Ireland?",
    ]
    titles = ["Sauron", "Irish Sea"]
    texts = [
        "Sala Baker is best known for portraying the villain Sauron in the Lord of the Rings trilogy.",
        "The Irish Sea is connected to the North Atlantic at both its northern and southern ends.",
    ]

    with torch.no_grad():
        q_vecs = model.encode_questions(questions)
        p_vecs = model.encode_passages(titles, texts)
        scores = model.similarity(q_vecs, p_vecs)

    print("question vectors:", tuple(q_vecs.shape))
    print("passage vectors :", tuple(p_vecs.shape))
    print("score matrix    :", tuple(scores.shape))
    print(scores)
