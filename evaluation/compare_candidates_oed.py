## -*- coding: utf-8 -*-
# Author: Barbara McGillivray
# Date: 25/04/2018
# Python version: 3
# Script version: 1.0
# Script for evaluating the candidate words for semantic change outputted by the Temporal
# Random Indexing semantic change detection algorithm against OED words

# ----------------------------
# Initialization
# ----------------------------


# Import modules:

import requests
import os
import csv

# Parameters:

freq_filter = 100  # frequency filter for candidate words
method = "cum"  # alternative: "point" and "cum"
pvalue = "095"  # alternatives: "001", "01", and "05

# Directory and file names:

dir = os.path.join("/Users", "bmcgillivray", "Documents", "OneDrive", "The Alan Turing Institute",
                   "Visiting researcher Basile McGillivray - Documents")
dir_in = os.path.join(dir, "Evaluation", "OED")
dir_out = os.path.join(dir, "Evaluation", "output")
#oed_words_file_name = method + "_" + pvalue + "_words_for_lookup_freq_" + str(freq_filter) + "_oed.tsv"
oed_senses_file_name = "oed.sense"
oed_quotations_file_name = "oed.quotation"
# candidate words for semantic change detection, checked against OED API
file_out_name = method + "_" + pvalue + "_words_for_lookup_freq_" + str(freq_filter) + "_oed_evaluation.tsv"
file_out_name_summary = method + "_" + pvalue + "_words_for_lookup_freq_" + str(freq_filter) + "_oed_evaluation_summary.txt"
candidate_words_file_name = method + "_" + pvalue + "_words_for_lookup_freq_" + str(freq_filter) + ".txt"

# create output directory if it doesn't exist:
if not os.path.exists(dir_out):
    os.makedirs(dir_out)

# --------------------------------------------------------------
# Check changepoint year of candidate words against OED years
# --------------------------------------------------------------

# Read OED content:

oed_senses_file = open(os.path.join(dir_in, oed_senses_file_name), 'r')
oed_senses_reader = csv.reader(oed_senses_file, delimiter='\t')  # , quotechar='|')

row_count = sum(1 for row in oed_senses_reader)
oed_senses_file.close()

oed_senses_file = open(os.path.join(dir_in, oed_senses_file_name), 'r')
oed_senses_reader = csv.reader(oed_senses_file, delimiter='\t')  # , quotechar='|')

count = 0
oed_lemmapos2years = dict()  # maps OED lemma_pos to list of years from OED
oed_lemmapos2year2senseid = dict() # maps OED lemma_pos and year to the list of its sense ids
oed_senseid2def = dict() # maps OED sense id to its definition

gold_standard_lemmapos = list() # list of words that have changed meaning according to the OED between 1995 and 2014

for row in oed_senses_file:  # , max_col=5, max_row=max_number+1):
    count += 1
    fields = row.split("\t")
    #print(str(fields))
    #print(str(type(fields)))
    #print(str(fields[3]))
    if count > 1:
        #print("OED senses: count", str(count), " out of ", str(row_count))
        #word = row[0]
        #lemma = row[1]
        oed_senseid = fields[0]
        oed_lemma = fields[1]
        if oed_lemma != "NA":# and oed_lemma == "blackberry":
            oed_postag = fields[2]
            oed_lemmapos = oed_lemma + "_" + oed_postag
            oed_startdate = int(fields[3])
            oed_definition = fields[4]
            oed_senseid2def[oed_senseid] = oed_definition
            if oed_startdate >=1995 and oed_startdate <= 2014:
                gold_standard_lemmapos.append(oed_lemmapos)

            #if oed_lemmapos in oed_lemmapos2years:
            if oed_lemmapos in oed_lemmapos2years:
                years = oed_lemmapos2years[oed_lemmapos]
                #years = oed_lemmapos2years[oed_lemma]
                years.append(oed_startdate)
                oed_lemmapos2years[oed_lemmapos] = years
                #oed_lemmapos2years[oed_lemma] = years
            else:
                oed_lemmapos2years[oed_lemmapos] = [oed_startdate]
                #oed_lemmapos2years[oed_lemma] = [oed_startdate]

            if oed_lemmapos in oed_lemmapos2year2senseid:
                senseids = oed_lemmapos2year2senseid[oed_lemmapos]
                senseids.append(oed_senseid)
                oed_lemmapos2year2senseid[oed_lemmapos] = senseids
            else:
                oed_lemmapos2year2senseid[oed_lemmapos] = [oed_senseid]


oed_senses_file.close()

# Read OED quotations and map them to each sense id:

oed_quotations_file = open(os.path.join(dir_in, oed_quotations_file_name), 'r')
oed_quotations_reader = csv.reader(oed_quotations_file, delimiter='\t')  # , quotechar='|')

row_count = sum(1 for row in oed_quotations_reader)
oed_quotations_file.close()

oed_quotations_file = open(os.path.join(dir_in, oed_quotations_file_name), 'r')
oed_quotations_reader = csv.reader(oed_quotations_file, delimiter='\t')  # , quotechar='|')

count = 0
oed_senseid2quotid = dict() # map each OED sense id to the list of its quotation ids
oed_quotid2quot = dict() # map each OED quotation id to its quotation


for row in oed_quotations_reader:  # , max_col=5, max_row=max_number+1):
    count += 1

    if count > 1:
        #print("OED quotations: count", str(count), " out of ", str(row_count))
        # word = row[0]
        # lemma = row[1]
        oed_quotid = row[0]
        oed_senseid = row[1]
        oed_date = row[2]
        oed_quotation = row[3]
        oed_quotid2quot[oed_quotid] = oed_quotation

        if oed_senseid in oed_senseid2quotid:
            quotids = oed_senseid2quotid[oed_senseid]
            quotids.append(oed_quotid)
            oed_senseid2quotid[oed_senseid] = quotids
        else:
            oed_senseid2quotid[oed_senseid] = [oed_quotid]

oed_quotations_file.close()

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
        #print("Corpus frequencies: count", str(count), " out of ", str(row_count))
        word = row[0]
        pos = row[1]
        lemma = row[2]
        lemmapos = lemma + "_" + pos
        frequency = row[3]
        changepoints = row[4]
        candidate2changepoints[lemmapos] = changepoints.split("_")
        #candidate2changepoints[lemma] = changepoints.split("_")

candidate_words_file.close()

# Compare the corpus changepoints  of a candidate lemma with its OED years:

correct_candidates = list()
candidates = list()
correct = 0

for lemmapos in candidate2changepoints:
    changepoints = candidate2changepoints[lemmapos]
    candidates.append(lemmapos)
    if lemmapos in oed_lemmapos2years:
        #print(lemmapos)
        oedyears = oed_lemmapos2years[lemmapos]
        #print("OED years:", oedyears)
        #print("Changepoints:", changepoints)
        correct = 0

        for changepoint in changepoints:
            #print("Changepoint:"+changepoint)
            for oedyear in oedyears:
                #print("Oed year:"+oedyear)
                #if int(changepoint) == int(oedyear) or int(changepoint) == int(oedyear) - 1 or int(changepoint) == int(oedyear) + 1:
                if int(changepoint) >= int(oedyear) and int(oedyear) >= 1995:
                    print("Correct changepoint for ", lemmapos, ": ", changepoint, oedyear)
                    correct = 1

        if correct == 1:
            correct_candidates.append(lemmapos)

correct_candidates = list(set(correct_candidates))

# Calculate precision, recall, and F-score:


print("Total number of correct candidates:", str(len(correct_candidates)))
print("Total number of candidates:", str(len(candidates)))
P = len(correct_candidates)/len(candidates)
print("Precision:", P)
print("Total number of gold standard lemmapos:", str(len(gold_standard_lemmapos)))
R = len(correct_candidates)/len(gold_standard_lemmapos)
print("Recall:", R)


# -----------------------------
# Print to output files:
# -----------------------------

# List of correct candidates, and their years:

output_file = open(os.path.join(dir_out, file_out_name), "w")

writer_output = csv.writer(output_file, delimiter='\t', quotechar='|', quoting=csv.QUOTE_MINIMAL,
                                lineterminator='\n')
writer_output.writerow(['correct_lemma', 'correct_pos', 'changepoints', 'oed_years'])

for correct in correct_candidates:
    lemma = correct.split("_")[0]
    pos = correct.split("_")[1]
    writer_output.writerow([lemma, pos, candidate2changepoints[correct], oed_lemmapos2years[correct]])

output_file.close()

# Evaluation statistics:

output_file_summmary = open(os.path.join(dir_out, file_out_name_summary), "w")
output_file_summmary.write("Precision:"+str(P)+"\n")
output_file_summmary.write("Recall:"+str(R))
output_file_summmary.close()

# ---------------------------------------------------------------------
# Compare OED vector for candidate words with their corpus neighbours
# ---------------------------------------------------------------------




r = requests.get('https://github.com/timeline.json')
r.json()
