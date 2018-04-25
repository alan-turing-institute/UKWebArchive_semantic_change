## -*- coding: utf-8 -*-
# Author: Barbara McGillivray
# Date: 25/04/2018
# Python version: 3
# Script version: 1.0
# Script for creating querying the OED API for evaluating the candidate words for semantic change outputted by the Temporal
# Random Indexing semantic change detection algorithm

# ----------------------------
# Initialization
# ----------------------------


# Import modules:

import requests
import os
import csv

# Parameters:

freq_filter = 100  # frequency filter for candidate words
method = "point"  # alternative: "point" and "cum"
pvalue = "001"  # alternatives: "001", "01", and "05

# Directory and file names:

dir = os.path.join("/Users", "bmcgillivray", "Documents", "OneDrive", "The Alan Turing Institute",
                   "Visiting researcher Basile McGillivray - Documents")
dir_in = os.path.join(dir, "Evaluation", "OED")
dir_out = os.path.join(dir, "Evaluation", "output")
oed_words_file_name = method + "_" + pvalue + "_words_for_lookup_freq_" + str(freq_filter) + "_oed.tsv"
# candidate words for semantic change detection, checked against OED API
file_out_name = method + "_" + pvalue + "_words_for_lookup_freq_" + str(freq_filter) + "_oed_evaluation.tsv"
candidate_words_file_name = method + "_" + pvalue + "_words_for_lookup_freq_" + str(freq_filter) + ".txt"

# create output directory if it doesn't exist:
if not os.path.exists(dir_out):
    os.makedirs(dir_out)

# --------------------------------------------------------------
# Check changepoint year of candidate words against OED years
# --------------------------------------------------------------

# Read OED content:

oed_words_file = open(os.path.join(dir_in, oed_words_file_name), 'r')
oed_words_reader = csv.reader(oed_words_file, delimiter='\t')  # , quotechar='|')

row_count = sum(1 for row in oed_words_reader)
oed_words_file.close()

oed_words_file = open(os.path.join(dir_in, oed_words_file_name), 'r')
oed_words_reader = csv.reader(oed_words_file, delimiter='\t')  # , quotechar='|')

count = 0
oed_lemmapos2years = dict()  # maps OED lemma_pos to list of years from OED

for row in oed_words_reader:  # , max_col=5, max_row=max_number+1):
    count += 1
    if count > 1:
        print("OED words: count", str(count), " out of ", str(row_count))
        word = row[0]
        lemma = row[1]
        oed_senseid = row[2]
        oed_lemma = row[3]
        if oed_lemma != "NA":
            oed_postag = row[4]
            oed_lemmapos = oed_lemma + "_" + oed_postag
            oed_startdate = row[5]
            oed_definition = row[6]
            #if oed_lemmapos in oed_lemmapos2years:
            if oed_lemma in oed_lemmapos2years:
                #years = oed_lemmapos2years[oed_lemmapos]
                years = oed_lemmapos2years[oed_lemma]
                years.append(oed_startdate)
                #oed_lemmapos2years[oed_lemmapos] = years
                oed_lemmapos2years[oed_lemma] = years
            else:
                #oed_lemmapos2years[oed_lemmapos] = [oed_startdate]
                oed_lemmapos2years[oed_lemma] = [oed_startdate]

oed_words_file.close()

# Read corpus changepoints for candidate words:

candidate_words_file = open(os.path.join(dir_out, candidate_words_file_name), 'r')
candidate_words_reader = csv.reader(candidate_words_file, delimiter='\t')  # , quotechar='|')

row_count = sum(1 for row in candidate_words_reader)
candidate_words_file.close()

candidate_words_file = open(os.path.join(dir_out, candidate_words_file_name), 'r')
candidate_words_reader = csv.reader(candidate_words_file, delimiter='\t')  # , quotechar='|')

count = 0

candidate2changepoints = dict()  # maps a candidate's lemma_pos to the list of its corpus changepoints

for row in candidate_words_reader:  # , max_col=5, max_row=max_number+1):
    count += 1
    if count > 1:
        print("Corpus frequencies: count", str(count), " out of ", str(row_count))
        word = row[0]
        pos = row[1]
        lemma = row[2]
        lemmapos = lemma + "_" + pos
        frequency = row[3]
        changepoints = row[4]
        #candidate2changepoints[lemmapos] = changepoints.split("_")
        candidate2changepoints[lemma] = changepoints.split("_")

candidate_words_file.close()

# Compare the corpus changepoints  of a candidate lemma with its OED years:

for lemmapos in candidate2changepoints:
    changepoints = candidate2changepoints[lemmapos]
    if lemmapos in oed_lemmapos2years and lemmapos.startswith("oligarch"):
        print(lemmapos)
        oedyears = oed_lemmapos2years[lemmapos]
        print("OED years:", oedyears)
        print("Changepoints:", changepoints)

        for changepoint in changepoints:
            print("Changepoint:"+changepoint)
            for oedyear in oedyears:
                print("Oed year:"+oedyear)
                if int(changepoint) == int(oedyear) or int(changepoint) == int(oedyear) - 1 or int(changepoint) == int(oedyear) + 1:
                    print("Correct changepoint for ", lemmapos, ": ", changepoint, oedyear)

# Calculate precision, recall, and F-score:



# Print to output file:


# ---------------------------------------------------------------------
# Compare OED vector for candidate words with their corpus neighbours
# ---------------------------------------------------------------------




r = requests.get('https://github.com/timeline.json')
r.json()
