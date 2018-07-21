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

import csv
import os
import datetime
import matplotlib.pyplot as plt
import nltk
from nltk.tokenize import WordPunctTokenizer  # tokenizer

now = datetime.datetime.now()
today_date = str(now)[:10]

# Parameters:

freq_filter = 100  # frequency filter for candidate words
changepoint_detection_values = ["simple_valley", "mean_shift"]
# ["valley_var_1", "valley_var_2", "valley_var_4", "simple_valley", "mean_shift"]
method_values = ["point", "cum"]#["occ", "point", "cum"]
pvalue_values = ["090", "095"] # under the "valley_var" approach we consider the number after of times the down-ward trend needs
# to be higher than the variance in order to lead to a changepoint
vector_values = ["inc"]#["sep", "inc"] # how the word vectors are built;
# "sep" means that the word space at a time point t is built separately without taking into account the previous
# space (at time t-1); "inc" means that the word spaces are built incrementally


# Directory and file names:

directory = os.path.join("/Users", "bmcgillivray", "Documents", "OneDrive", "The Alan Turing Institute",
                         "Visiting researcher Basile McGillivray - Documents")
dir_in = os.path.join(directory, "tri")
dir_out = os.path.join(directory, "Evaluation", "output")

# create output directory if it doesn't exist:
if not os.path.exists(dir_out):
    os.makedirs(dir_out)

english_terms_file_name = "words_alpha.txt"  # list of 400,000 English terms
oed_terms_file_name = "oed_words.tsv"
corpus_words_file_name = "dict.sample.all"

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
    # if count < 100:
    print("Corpus frequencies: count", str(count), " out of ", str(row_count))
    word = row[0]
    freq = int(row[1])
    word_freq[word] = freq

corpus_words_file.close()

# Read list of OED terms:

c = 0
english_terms_file = open(os.path.join(directory, "Evaluation", "OED", oed_terms_file_name))
english_terms = list()
for line in english_terms_file:
    c += 1
    if c > 1:
        fields = line.split("\t")
        term = fields[1]
        english_terms.append(term)

#english_terms = english_terms_file.read().splitlines()
english_terms_file.close()

# plot histogram of corpus frequencies of all the words:

histogram_corpus_filename = "histogram_corpus_freq_" + str(freq_filter) + ".png"

fig = plt.figure()
ax = fig.add_subplot(111)
numBins = 100
ax.hist(list(word_freq.values()), numBins, color='green')
fig.savefig(os.path.join(dir_out, histogram_corpus_filename))
plt.close(fig)

for changepoint_detection in changepoint_detection_values:

    var = ""
    print("Changepoint detection:", changepoint_detection)
    pvalue_values = ["090", "095"]
    if changepoint_detection.startswith("valley_var"):
        var = changepoint_detection.split("_")[2]
        pvalue_values = ["0"]
    else:
        pvalue_values = ["090", "095"]

    for vector in vector_values:
        if vector == "inc":
            method_values = ["point", "cum"]
            pvalue_values = ["095"]
        else:
            if changepoint_detection.startswith("valley_var"):
                pvalue_values = ["0"]
            else:
                pvalue_values = ["090", "095"]

        for method in method_values:
            print("Method:", method)

            for pvalue in pvalue_values:

                # candidate words for semantic change detection,
                # p-value = 0.0001, method = cumulative, dataset = 20% of Uk Web Archive JISC dataset 1996-2003

                file_out_name = method + "_words_for_lookup_freq_" + str(freq_filter) + "pvalue-" + str(pvalue) + \
                                "changepoint-detection_" + changepoint_detection + "_vector-" + vector + ".txt"
                histogram_0_filename = method + "_" + pvalue + "_histogram_corpus_freq_filtered0.png"
                histogram_1_filename = method + "_" + pvalue + "_histogram_corpus_freq_filtered1.png"

                if changepoint_detection.startswith("valley_var"):
                    histogram_0_filename = method + "_histogram_corpus_freq_filtered0.png"
                    histogram_1_filename = method + "_histogram_corpus_freq_filtered1.png"

                if vector == "sep":
                    if method == "occ":
                        if changepoint_detection == "simple_valley":
                            candidate_words_file_name = method + "_s20_year_CPD_baseline.csv"
                        elif changepoint_detection == "mean_shift":
                            candidate_words_file_name = method + "_s20_year_CPDv2_" + pvalue + "_down_label.csv"
                        else:
                            candidate_words_file_name = method + "_s20_year_var_" + var + ".csv"

                    else:
                        if changepoint_detection == "simple_valley":
                            candidate_words_file_name = "ukwac_s20_year_" + method + "_CPD_baseline.csv"
                        elif changepoint_detection == "mean_shift":
                            candidate_words_file_name = "ukwac_s20_year_" + method + "_CPDv2_" + pvalue + "_down_label.csv"
                        elif changepoint_detection.startswith("valley_var"):
                            candidate_words_file_name = "ukwac_s20_year_" + method + "_CPD_var_" + var + ".csv"
                else:
                    if changepoint_detection == "simple_valley":
                        candidate_words_file_name = "ukwac_all_" + vector + "_" + method + "_baseline.csv"
                    elif changepoint_detection == "mean_shift":
                        candidate_words_file_name = "ukwac_all_" + vector + "_" + method + "_CPDv2_" + pvalue + ".csv"
                    else:
                        candidate_words_file_name = "ukwac_all_" + vector + "_" + method + "_var" + var + ".csv"


            # ----------------------------------------------
            # Check candidate words against English terms
            # ----------------------------------------------


            # candidate words and their changepoints, each candidate is mapped to the list of its changepoints:
            candidate_words_changepoint = dict()

            # Read candidate words list and their changepoints:

            print("Reading candidate words list...")

            if vector == "inc":
                candidates_file = open(os.path.join(dir_in, "year", "inc", candidate_words_file_name), 'r')
            else:
                if method == "occ":
                    candidates_file = open(os.path.join(dir_in, "year", "v2", "occ", candidate_words_file_name), 'r')
                else:
                    candidates_file = open(os.path.join(dir_in, "year", "v2", candidate_words_file_name), 'r')

            candidates_reader = csv.reader(candidates_file, delimiter='\t')  # , quotechar='|')

            row_count = sum(1 for row in candidates_reader)
            candidates_file.close()

            if vector == "inc":
                candidates_file = open(os.path.join(dir_in, "year", "inc", candidate_words_file_name), 'r')
            else:
                if method == "occ":
                    candidates_file = open(os.path.join(dir_in, "year", "v2", "occ", candidate_words_file_name), 'r')
                else:
                    candidates_file = open(os.path.join(dir_in, "year", "v2", candidate_words_file_name), 'r')

            candidates_reader = csv.reader(candidates_file, delimiter='\t')  # , quotechar='|')

            print("file:", str(candidate_words_file_name))
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

            # For every candidate word, select it if it occurs in the list of 400,000 English terms:

            candidates_filtered0 = list()

            print("Filtering candidates against list of English terms...")

            count = 0
            for candidate in candidate_words_changepoint:
                count += 1
                print("Filter 1, count: " + str(count), " out of ", str(len(candidate_words_changepoint)))
                if candidate in english_terms:
                    candidates_filtered0.append(candidate)

            # print(str(candidates_filtered0))


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

            fig = plt.figure()
            ax = fig.add_subplot(111)
            numBins = 100
            ax.hist(list(word_freq_filtered0.values()), numBins, color='green')
            fig.savefig(os.path.join(dir_out, histogram_0_filename))
            plt.close(fig)

            # plot histogram of corpus frequencies of the candidates filtered against English terms and frequency filter:

            # print(str(word_freq_filtered1))

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
                # new_pos = ""

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


            # stop_words = set(stopwords.words("english"))  # setting and selecting stopwords to be in english

            tokenizer = WordPunctTokenizer()  # assigning WordPunctTokenizer function to be a variable.

            # tokens = nltk.word_tokenize(candidates_filtered1)
            pos = nltk.pos_tag(candidates_filtered1)
            # print(str(pos))

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
                writer_output.writerow(
                        [candidates_filtered1[i], pos[i][1], lemmas[i], word_freq_filtered1[candidates_filtered1[i]],
                         changepoints])

            output_file.close()
