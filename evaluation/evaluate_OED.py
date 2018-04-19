## -*- coding: utf-8 -*-
# Author: Barbara McGillivray
# Date: 19/04/2018
# Python version: 3
# Script version: 1.0
# Script for evaluating the candidate words for semantic change outputted by the Temporal Random Indexing semantic
# change detection algorithm against the OED

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

# Directory and file names:

dir = os.path.join("/Users", "bmcgillivray", "Documents", "OneDrive", "The Alan Turing Institute",
                   "Visiting researcher Basile McGillivray - Documents")
dir_in = os.path.join(dir, "tri")
dir_out = os.path.join(dir, "Evaluation", "output")
english_terms_file = "words_alpha.txt" # list of 400,000 English terms
candidate_words_file = "ukwac_s20_year_cum_CPD_001_label.csv" # candidate words for semantic change detection,
# p-value = 0.0001, method = cumulative, dataset = 20% of Uk Web Archive JISC dataset 1996-2003
corpus_words_file = "dict.sample.all"

# create output directory if it doesn't exist:
if not os.path.exists(dir_out):
    os.makedirs(dir_out)



# --------------------------------------------
# Parameters
# --------------------------------------------

freq_filter = 100 # frequency filter for candidate words

# --------------------------------------------
# Analyse distribution of corpus frequencies
# --------------------------------------------


# Word frequencies:
word_freq = dict()

# Read corpus words and their corpus frequencies:

print("Reading corpus frequencies...")

corpus_words_file = open(os.path.join(dir_in, corpus_words_file), 'r')
corpus_words_reader = csv.reader(corpus_words_file, delimiter='\t')  # , quotechar='|')

count = 0
for row in corpus_words_reader:  # , max_col=5, max_row=max_number+1):
    count += 1
    #if count < 100:
    print("count", str(count))
    word = row[0]
    freq = int(row[1])
    word_freq[word] = freq

# plot histogram of corpus frequencies of all the words:

fig = plt.figure()
ax = fig.add_subplot(111)
numBins = 100
ax.hist(list(word_freq.values()), numBins, color='green')
fig.savefig(os.path.join(dir_out, "histogram_corpus_freq.png"))
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

candidates_file = open(os.path.join(dir_in, "year", candidate_words_file), 'r')
candidates_reader = csv.reader(candidates_file, delimiter='\t')  # , quotechar='|')

count = 0
for row in candidates_reader:  # , max_col=5, max_row=max_number+1):
    count += 1
    print("count", str(count))
    word = row[0]
    changepoint = row[1]
    if word in candidate_words_changepoint:
        changepoints_word = candidate_words_changepoint[word]
        changepoints_word.append(changepoint)
        candidate_words_changepoint[word] = changepoints_word
    else:
        candidate_words_changepoint[word] = [changepoint]


# Read list of English terms:

english_terms_file = open(os.path.join(dir_in, english_terms_file))
english_terms = english_terms_file.read().splitlines()

# For every candidate word, select it if it occurs in the list of 400,000 English terms:

candidates_filtered0 = list()

print("Filtering candidates against list of English terms...")

for candidate in candidate_words_changepoint:
    #print(candidate)
    if candidate in english_terms:
        candidates_filtered0.append(candidate)

print(str(candidates_filtered0))


# For every candidate word selected in ii, look up its corpus frequency in dict.sample.all and filter it at 100

word_freq_filtered0 = dict()
word_freq_filtered1 = dict()

for word in candidates_filtered0:
    if word in word_freq:
        word_freq_filtered0[word] = word_freq[word]
        if word_freq[word] > freq_filter:
            ...


# plot histogram of corpus frequencies of the candidates filtered against English terms:

print(str(word_freq_filtered0))

fig = plt.figure()
ax = fig.add_subplot(111)
numBins = 100
ax.hist(list(word_freq_filtered0.values()), numBins, color='green')
fig.savefig(os.path.join(dir_out, "histogram_corpus_freq_filtered0.png"))
plt.close(fig)




# Initialize lemmatizer:

#lemmatizer = LemmaReplacer('latin')

# Read input files:

#print(str((dir_in)))
#files = [f for f in listdir(dir_in) if isfile(join(dir_in, f)) and f.endswith('_la')]

#for file in files:

#    print("file:" + file)
#    text = codecs.open(os.path.join(dir_in, file), 'r').read()
