import sentencepiece as spm
from transformers import PreTrainedTokenizerFast


class CustomSentencePieceBPE(PreTrainedTokenizerFast):
    def __init__(self, model_path):
        super().__init__()
        self.sp = spm.SentencePieceProcessor()
        self.sp.load(model_path)

    def _tokenize(self, text):
        return self.sp.encode_as_pieces(text)

    def _convert_token_to_id(self, token):
        return self.sp.piece_to_id(token)

    def _convert_id_to_token(self, index):
        return self.sp.id_to_piece(index)

    def convert_tokens_to_string(self, tokens):
        return self.sp.decode_pieces(tokens)

    def _encode_plus(
        self,
        text,
        text_pair=None,
        add_special_tokens=True,
        padding_strategy=None,
        max_length=None,
        stride=0,
        is_split_into_words=False,
        pad_to_multiple_of=None,
        return_tensors=None,
        **kwargs
    ):
        return self.sp.encode(text, out_type=int)
