# coding:utf-8

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer

# 语料
corpus = [
    'This is the first document.',
    'This is the second document.',
    'And the third one.',
    'Is this the first document?'
]
# 将文本中的词语转换为词频矩阵
vectorizer = CountVectorizer()
# 计算个词语出现的次数
X = vectorizer.fit_transform(corpus)
# 获取词袋中所有文本关键词
word = vectorizer.get_feature_names()
print word
# 查看词频结果
print X.toarray()

transformer = TfidfTransformer()
print transformer
tfidf = transformer.fit_transform(X)
# 查看数据结构 tfidf[i][j]表示i类文本中的tf-idf权重
print tfidf.toarray()
