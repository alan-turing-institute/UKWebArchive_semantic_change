## -*- coding: utf-8 -*-
# Author: Barbara McGillivray
# Date: 19/04/2018
# Python version: 3
# Script version: 1.0
# Script for creating wordlists used for evaluating the candidate words for semantic change outputted by the Temporal
# Random Indexing semantic change detection algorithm against the OED

# ----------------------------
# Initialization
# ----------------------------


# Import modules:

import codecs
import os
import csv
from os import listdir
from os.path import isfile, join
from cltk.stem.lemma import LemmaReplacer
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
from nltk.tokenize import sent_tokenize
import re
import nltk
from nltk.tokenize import WordPunctTokenizer  # tokenizer
from nltk.stem import SnowballStemmer  # stemmer
from nltk.corpus import stopwords  # stopwords


# Parameters:

freq_filter = 100 # frequency filter for candidate words
method = "cum" # alternative: "point" and "cum"
pvalue = "01" # alternatives: "001", "01", and "05


# Directory and file names:

dir = os.path.join("/Users", "bmcgillivray", "Documents", "OneDrive", "The Alan Turing Institute",
                   "Visiting researcher Basile McGillivray - Documents")
dir_in = os.path.join(dir, "tri")
dir_out = os.path.join(dir, "Evaluation", "output")
english_terms_file_name = "words_alpha.txt" # list of 400,000 English terms
candidate_words_file_name = "ukwac_s20_year_"+method+"_CPD_"+pvalue+"_label.csv" # candidate words for semantic change detection,
# p-value = 0.0001, method = cumulative, dataset = 20% of Uk Web Archive JISC dataset 1996-2003
corpus_words_file_name = "dict.sample.all"
file_out_name = method + "_" + pvalue + "_words_for_lookup_freq_"+str(freq_filter)+".txt"
histogram_corpus_filename = method + "_" + pvalue + "_histogram_corpus_freq.png"
histogram_0_filename = method + "_" + pvalue + "_histogram_corpus_freq_filtered0.png"
histogram_1_filename = method + "_" + pvalue + "_histogram_corpus_freq_filtered1.png"

# create output directory if it doesn't exist:
if not os.path.exists(dir_out):
    os.makedirs(dir_out)



# --------------------------------------------
# Analyse distribution of corpus frequencies
# --------------------------------------------


# Word frequencies:
word_freq = dict()

# Read corpus words and their corpus frequencies:

print("Reading corpus frequencies...")

corpus_words_file = open(os.path.join(dir_in, corpus_words_file_name), 'r')
corpus_words_reader = csv.reader(corpus_words_file, delimiter='\t')  # , quotechar='|')

row_count = sum(1 for row in corpus_words_reader)
corpus_words_file.close()

corpus_words_file = open(os.path.join(dir_in, corpus_words_file_name), 'r')
corpus_words_reader = csv.reader(corpus_words_file, delimiter='\t')  # , quotechar='|')
count = 0
for row in corpus_words_reader:  # , max_col=5, max_row=max_number+1):
    count += 1
    #if count < 100:
    print("Corpus frequencies: count", str(count), " out of ", str(row_count))
    word = row[0]
    freq = int(row[1])
    word_freq[word] = freq

corpus_words_file.close()

# plot histogram of corpus frequencies of all the words:

fig = plt.figure()
ax = fig.add_subplot(111)
numBins = 100
ax.hist(list(word_freq.values()), numBins, color='green')
fig.savefig(os.path.join(dir_out, histogram_corpus_filename))
plt.close(fig)


# ----------------------------------------------
# Check candidate words against English terms
# ----------------------------------------------


# candidate words and their changepoints, each candidate is mapped to the list of its changepoints:
candidate_words_changepoint = dict()

# English terms:
english_terms = list()

# Read candidate words list and their changepoints:

print("Reading candidate words list...")

candidates_file = open(os.path.join(dir_in, "year", candidate_words_file_name), 'r')
candidates_reader = csv.reader(candidates_file, delimiter='\t')  # , quotechar='|')

row_count = sum(1 for row in candidates_reader)
candidates_file.close()

candidates_file = open(os.path.join(dir_in, "year", candidate_words_file_name), 'r')
candidates_reader = csv.reader(candidates_file, delimiter='\t')  # , quotechar='|')

count = 0
for row in candidates_reader:  # , max_col=5, max_row=max_number+1):
    count += 1
    print("Candidate: count", str(count), " out of ", str(row_count))
    word = row[0]
    changepoint = row[1]
    if word in candidate_words_changepoint:
        changepoints_word = candidate_words_changepoint[word]
        changepoints_word.append(changepoint)
        candidate_words_changepoint[word] = changepoints_word
    else:
        candidate_words_changepoint[word] = [changepoint]

candidates_file.close()

# Read list of English terms:

english_terms_file = open(os.path.join(dir_in, english_terms_file_name))
english_terms = english_terms_file.read().splitlines()

# For every candidate word, select it if it occurs in the list of 400,000 English terms:

candidates_filtered0 = list()

print("Filtering candidates against list of English terms...")

count = 0
for candidate in candidate_words_changepoint:
    count += 1
    print("Filter 1, count: "+str(count), " out of ", str(len(candidate_words_changepoint)))
    if candidate in english_terms:
        candidates_filtered0.append(candidate)

english_terms_file.close()
#print(str(candidates_filtered0))


# For every candidate word selected in ii, look up its corpus frequency in dict.sample.all and filter it at 100

print("Filtering candidates against list of English terms...")
candidates_filtered1 = list()
word_freq_filtered0 = dict()
word_freq_filtered1 = dict()

count = 0
for word in candidates_filtered0:
    count += 1
    print("Filter 2, count: " + str(count), " out of ", str(len(candidates_filtered0)))
    if word in word_freq:
        word_freq_filtered0[word] = word_freq[word]
        if word_freq[word] > freq_filter:
            word_freq_filtered1[word] = word_freq[word]
            candidates_filtered1.append(word)


# plot histogram of corpus frequencies of the candidates filtered against English terms:

#print(str(word_freq_filtered0))

fig = plt.figure()
ax = fig.add_subplot(111)
numBins = 100
ax.hist(list(word_freq_filtered0.values()), numBins, color='green')
fig.savefig(os.path.join(dir_out, histogram_0_filename))
plt.close(fig)

# plot histogram of corpus frequencies of the candidates filtered against English terms and frequency filter:

#print(str(word_freq_filtered1))

fig = plt.figure()
ax = fig.add_subplot(111)
numBins = 100
ax.hist(list(word_freq_filtered1.values()), numBins, color='green')
fig.savefig(os.path.join(dir_out, histogram_1_filename))
plt.close(fig)


# ------------------------------------
# Lemmatization
# ------------------------------------

print("Lemmatizing...")

def get_new_pos(old_pos):

    new_pos = ""

    if old_pos.startswith('J'):
        new_pos = "a"
    elif old_pos.startswith('V'):
        new_pos = "v"
    elif old_pos.startswith('N'):
        new_pos = "n"
    elif old_pos.startswith('R'):
        new_pos = "r"
    else:
        new_pos = ""

    return new_pos

#stop_words = set(stopwords.words("english"))  # setting and selecting stopwords to be in english

tokenizer = WordPunctTokenizer()  # assigning WordPunctTokenizer function to be a variable.

#tokens = nltk.word_tokenize(candidates_filtered1)
pos = nltk.pos_tag(candidates_filtered1)
#print(str(pos))

wordnet_lemmatizer = nltk.WordNetLemmatizer()
lemmas = [wordnet_lemmatizer.lemmatize(t) for t in candidates_filtered1]


# ------------------------------------
# Create output
# ------------------------------------

print("Writing to output file...")

# list of candidate words to look up in OED with their pos, lemma, corpus frequency, and changepoint year:
output_file = open(os.path.join(dir_out, file_out_name), "w")

writer_output = csv.writer(output_file, delimiter='\t', quotechar='|', quoting=csv.QUOTE_MINIMAL,
                                lineterminator='\n')
writer_output.writerow(['word', 'pos', 'lemma', 'frequency', 'changepoints'])

for i in range(len(candidates_filtered1)):
    print("Writing word, count: ", str(i), " out of ", str(len(candidates_filtered1)))
    changepoints = "_".join(candidate_words_changepoint[candidates_filtered1[i]])
    writer_output.writerow([candidates_filtered1[i], pos[i][1], lemmas[i], word_freq_filtered1[candidates_filtered1[i]],
                            changepoints])

