## -*- coding: utf-8 -*-
# Author: Barbara McGillivray
# Date: 25/04/2018
# Python version: 3
# Script version: 1.0
# Script for evaluating the candidate words for semantic change outputted by the Temporal
# Random Indexing semantic change detection algorithm against OED words, and checking their corpus neighbours against
# the words occurring in the OED definitions and quotations.

# NB: the bit about neighbours needs to be completed!!

# ----------------------------
# Initialization
# ----------------------------


# Import modules:

# import requests
import os
import csv
from nltk import word_tokenize
from nltk.corpus import stopwords

stop_words = set(stopwords.words('english'))

# Parameters:

freq_filter = 100  # frequency filter for candidate words
method_values = ["point"]  # ["point", "cum", "occ"]
pvalue_values = ["090"]  # ["090", "095"] # NB: if simple_valley_baseline = "yes", this parameter is ignored
freq_threshold = 500  # Frequency threshold for allowing a word into the corpus dictionary
changepoint_detection_values = [
    "simple_valley"]  # ["simple_valley", "mean_shift", "valley_var_1", "valley_var_2", "valley_var_4"]
year_window_values = [
    "greater"]  # ["greater", "2", "3"]  # this is the size of the window of years for matching changepoints to OED
# years;
# if "greater", then we match any changepoint that is >= OED year
lemmatization_values = ["yes", "no"]

# Directory and file names:

directory = os.path.join("/Users", "bmcgillivray", "Documents", "OneDrive", "The Alan Turing Institute",
                         "Visiting researcher Basile - Documents")
dir_in = os.path.join(directory, "Evaluation", "OED")
dir_out = os.path.join(directory, "Evaluation", "output")

# create output directory if it doesn't exist:
if not os.path.exists(dir_out):
    os.makedirs(dir_out)

# Input files:
oed_senses_file_name = "oed.sense"
oed_quotations_file_name = "oed.quotation"
dict_file_name = "dict.sample.all"
neighbour_file_name = "neighborhood_100.csv"

file_out_all_name = "oed_evaluation_overview.csv"

# Read corpus neighbours:

neighbour_file = open(os.path.join(directory, "tri", "v2", "neighborhood_100",
                                   neighbour_file_name), 'r')

row_count_neighbour = sum(1 for line in neighbour_file)
neighbour_file.close()

# read list of neighbours from corpus:

neighbour_file = open(os.path.join(directory, "tri", "v2", "neighborhood_100",
                                   neighbour_file_name), 'r')
count_n = 0

neighbour_words = dict()  # maps a candidate and a year to the list of its top 100 corpus neighbours

for line in neighbour_file:
    count_n += 1
    print("Neighbour dictionary: count", str(count_n), " out of ",
          str(row_count_neighbour))
    fields = line.split("\t")
    word = fields[0]
    year = int(fields[1])
    neighbours = fields[2:102]
    neighbour_words[word, year] = neighbours

neighbour_file.close()

# Read corpus frequencies:

dict_file = open(os.path.join(directory, "tri", dict_file_name), 'r')
dict_reader = csv.reader(dict_file, delimiter='\t')  # , quotechar='|')

row_count = sum(1 for row in dict_reader)
dict_file.close()

dict_file = open(os.path.join(directory, "tri", dict_file_name), 'r')
dict_reader = csv.reader(dict_file, delimiter='\t')  # , quotechar='|')

count = 0

dict_words = list()  # maps a candidate's lemma_pos to the list of its corpus changepoints

for row in dict_reader:  # , max_col=5, max_row=max_number+1):
    count += 1
    if count > 1:
        print("Corpus dictionary: count", str(count), " out of ", str(row_count))
        word = row[0]
        freq = int(row[1])
        if freq > freq_threshold:
            dict_words.append(word)

dict_file.close()

# Read OED quotations and map them to each sense id:

oed_quotations_file = open(os.path.join(dir_in, oed_quotations_file_name), 'r')
oed_quotations_reader = csv.reader(oed_quotations_file, delimiter='\t')  # , quotechar='|')

row_count = sum(1 for row in oed_quotations_reader)
oed_quotations_file.close()

oed_quotations_file = open(os.path.join(dir_in, oed_quotations_file_name), 'r')
oed_quotations_reader = csv.reader(oed_quotations_file, delimiter='\t')  # , quotechar='|')

count = 0
oed_senseid2quotid = dict()  # map each OED sense id to the list of its quotation ids
oed_quotid2quot = dict()  # map each OED quotation id to its quotation

for row in oed_quotations_reader:  # , max_col=5, max_row=max_number+1):
    count += 1

    if count > 1:
        print("OED quotations: count", str(count), " out of ", str(row_count))
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

# Read OED content:

oed_senses_file = open(os.path.join(dir_in, oed_senses_file_name), 'r')
oed_senses_reader = csv.reader(oed_senses_file, delimiter='\t')  # , quotechar='|')

row_count = sum(1 for row in oed_senses_reader)
oed_senses_file.close()

oed_senses_file = open(os.path.join(dir_in, oed_senses_file_name), 'r')
oed_senses_reader = csv.reader(oed_senses_file, delimiter='\t')  # , quotechar='|')

count = 0
oed_lemmapos2years = dict()  # maps OED lemma_pos that is also candidate to list of years from OED
oed_lemmapos2year2senseid = dict()  # maps OED lemma_pos (that is also candidate) and year to the list of its sense ids
oed_lemma2years = dict()  # maps OED lemma that is also candidate to list of years from OED
oed_lemma2year2senseid = dict()  # maps OED lemma that is also candidate and year to the list of its sense ids
oed_senseid2def = dict()  # maps OED sense id (of a lemma that is also candidate) to its definition

gold_standard_lemmapos = list()  # list of [words, pos] that have changed meaning according to the OED between 1995
# and 2014
# and that appear in the corpus above the frequency threshold
gold_standard_lemma = list()  # list of words that have changed meaning according to the OED between 1995 and 2014
# and that appear in the corpus above the frequency threshold

for row in oed_senses_file:  # , max_col=5, max_row=max_number+1):
    count += 1
    fields = row.split("\t")
    # print(str(fields))
    # print(str(type(fields)))
    # print(str(fields[3]))
    if count > 1:
        print("OED senses: count", str(count), " out of ", str(row_count))
        # word = row[0]
        # lemma = row[1]
        oed_senseid = fields[0]
        oed_lemma = fields[1]
        if oed_lemma != "NA":  # and oed_lemma == "blackberry":

            oed_postag = fields[2]
            oed_lemmapos = oed_lemma + "_" + oed_postag
            oed_startdate = int(fields[3])
            oed_definition = fields[4]
            oed_senseid2def[oed_senseid] = oed_definition

            if oed_startdate >= 1995 <= 2014 and oed_lemma in dict_words:

                gold_standard_lemmapos.append(oed_lemmapos)

                # OED lemma, pos pairs:

                if oed_lemmapos in oed_lemmapos2years:
                    years = oed_lemmapos2years[oed_lemmapos]
                    # years = oed_lemmapos2years[oed_lemma]
                    years.append(oed_startdate)
                    oed_lemmapos2years[oed_lemmapos] = years
                    # oed_lemmapos2years[oed_lemma] = years
                else:
                    oed_lemmapos2years[oed_lemmapos] = [oed_startdate]
                    # oed_lemmapos2years[oed_lemma] = [oed_startdate]

                if [oed_lemmapos, oed_startdate] in oed_lemmapos2year2senseid:
                    senseids = oed_lemmapos2year2senseid[oed_lemmapos, oed_startdate]
                    senseids.append(oed_senseid)
                    oed_lemmapos2year2senseid[oed_lemmapos, oed_startdate] = senseids
                else:
                    oed_lemmapos2year2senseid[oed_lemmapos, oed_startdate] = [oed_senseid]

                # OED lemmas:

                gold_standard_lemma.append(oed_lemma)

                if oed_lemma in oed_lemma2years:
                    years = oed_lemma2years[oed_lemma]
                    # years = oed_lemmapos2years[oed_lemma]
                    years.append(oed_startdate)
                    oed_lemma2years[oed_lemma] = years
                    # oed_lemmapos2years[oed_lemma] = years
                else:
                    oed_lemma2years[oed_lemma] = [oed_startdate]
                    # oed_lemmapos2years[oed_lemma] = [oed_startdate]

                if [oed_lemma, oed_startdate] in oed_lemma2year2senseid:
                    senseids = oed_lemma2year2senseid[oed_lemma]
                    senseids.append(oed_senseid)
                    oed_lemma2year2senseid[oed_lemma, oed_startdate] = senseids
                else:
                    oed_lemma2year2senseid[oed_lemma, oed_startdate] = [oed_senseid]

oed_senses_file.close()


# Function that compares a list of change points from the corpus with a list of years from OED:

def compare_years(year_window_par, changepoints_list, oedyears_list):
    num_correct = 0
    matching_oedyears_list = list()

    for c in changepoints_list:
        print("\tChangepoint:" + str(c))
        for oedyear in oedyears_list:
            print("\t\tOed year:" + str(oedyear))
            # if int(changepoint) == int(oedyear) or int(changepoint) == int(oedyear) - 1
            # or int(changepoint) == int(oedyear) + 1:
            if year_window_par == "greater":
                if int(c) >= int(oedyear) >= 1995:
                    print("\t\t\tCorrect changepoint (greater) for ", l, ": ", str(c), str(oedyear))
                    num_correct = 1
                    matching_oedyears_list.append(oedyear)
            else:
                if (int(c) >= int(oedyear) - int(year_window_par) and int(c) <= int(oedyear) + int(
                        year_window_par)) \
                        and int(oedyear) >= 1995:
                    print("\t\t\tCorrect changepoint (window) for ", l, ": ", str(c), str(oedyear))
                    num_correct = 1
                    matching_oedyears_list.append(oedyear)

    return [num_correct, matching_oedyears_list]


# Function that calculates the overlap between corpus neighbours and OED definition+quotation for a given matching year:

def calculate_overlap(lemmatization, l, changepoint_year, matching_oed_years_list, oed_years_list):
    overlap = 0  # number of words shared between corpus neighbours of l at changeppoint_year
    # and preprocessed OED definition+quotation words at matching year
    oed_words = 0  # number of preprocessed OED definition+quotation words for matching years and l
    neigh_words = 0  # number of corpus neighbours of l
    overlaps = list()  # list of number of words shared between corpus neighbours of l at changeppoint_year
    # and preprocessed OED definition+quotation words in any year >= 1995
    max_overlap = 0  # maximum among number of words shared between corpus neighbours of l at changeppoint_year
    # and preprocessed OED definition+quotation words in any year >= 1995

    if lemmatization == "yes":
        lemma = l.split("_")[0]
        pos = l.split("_")[1]
    else:
        lemma = l

    if [lemma, changepoint_year] in neighbour_words:
        neighbours = neighbour_words[lemma, changepoint_year]

    # retrieve definitions of lemma (or lemma_pos) in OED for each of the years and for the matching years:

    oed_content_words_all_years = dict()  # maps a year to the list of words in each OED definitions+quotations for all
    # years for this lemma
    oed_content_words_matching_years = list()  # list of words in all OED definitions+quotations for the matching years
    # for this lemma

    for year in oed_years_list:

        if lemmatization == "yes":
            oed_senseids = oed_lemmapos2year2senseid[l, year]
        else:
            oed_senseids = oed_lemma2year2senseid[l, year]

        for oed_senseid in oed_senseids:

            oed_definition = oed_senseid2def[oed_senseid]
            oed_definition_words = word_tokenize(oed_definition)
            oed_words_list = list()

            for w in oed_definition_words:
                w = w.lower()
                if w not in stop_words:
                    oed_words_list.append(w)
                    if year in matching_oedyears:
                        oed_content_words_matching_years.append(w)

            # retrieve quotations of this sense id in OED:

            if oed_senseid in oed_senseid2quotid:
                quot_ids = oed_senseid2quotid[oed_senseid]
                for quotid in quot_ids:
                    if quotid in oed_quotid2quot:
                        quotation = oed_quotid2quot[quotid]
                        oed_quotation_words = word_tokenize(quotation)
                        for w in oed_quotation_words:
                            w = w.lower()
                            if w not in stop_words:
                                oed_words_list.append(w)

                            if year in matching_oedyears:
                                oed_content_words_matching_years.append(w)

            oed_content_words_all_years[year] = list(set(oed_words_list))

    oed_content_words_matching_years = list(set(oed_content_words_matching_years))

    # Compare corpus neighbours with OED:

    neighbours = neighbour_words[lemma, changepoint_year]
    neighbours = list(set(neighbours))

    for neighbour_word in neighbours:
        neigh_words += 1

        # if the neighbour word is contained in the OED definitions+quotations of l in the matching years:
        if neighbour_word in oed_content_words_matching_years:
            overlap += 1

        # if the neighbour word is contained the OED definitions+quotations of l in one (or more) of the years:

        for year in oed_years_list:
            overlap_year = 0
            if year in oed_content_words_all_years:
                oed_w = oed_content_words_all_years[year]
                if neighbour_word in oed_w:
                    overlap_year += 1

            if max_overlap < overlap_year:
                max_overlap = overlap_year

    return [overlap, oed_words, neigh_words, max_overlap]


# -----------------------------------------
# Write to output
# -----------------------------------------

with open(os.path.join(dir_out, file_out_all_name), "w") as output_all_file:
    writer_all_output = csv.writer(output_all_file, delimiter='\t', quotechar='|', quoting=csv.QUOTE_MINIMAL,
                                   lineterminator='\n')
    writer_all_output.writerow(['method', 'changepoint detection', 'significance', 'year_window', 'lemmatization',
                                'num correct', 'num candidates', 'num gold standard', 'Precision', 'Recall',
                                'F1-score'])

    for lemmatization in lemmatization_values:

        for changepoint_detection in changepoint_detection_values:
            if changepoint_detection.startswith("valley_var"):
                method_values = ["point", "cum"]
                pvalue_values = ["0"]

            for method in method_values:
                for pvalue in pvalue_values:

                    for year_window in year_window_values:

                        candidate_words_file_name = method + "_words_for_lookup_freq_" + str(freq_filter) + "pvalue-" + \
                                                    str(
                                                        pvalue) + "changepoint-detection_" + changepoint_detection + ".txt"

                        if changepoint_detection.startswith("valley_var"):
                            candidate_words_file_name = method + "_words_for_lookup_freq_" + str(freq_filter) + \
                                                        "changepoint-detection_" + changepoint_detection + ".txt"
                        # oed_words_file_name = method + "_" + pvalue + "_words_for_lookup_freq_" + str(freq_filter) + "_oed.tsv"

                        # Output files: candidate words for semantic change detection, checked against OED API:

                        file_out_name = method + '_words_freq-' + str(freq_filter) + "pvalue-" + str(
                            pvalue) + "_yearwindow-" + str(year_window) \
                                        + "lemmatization-" + lemmatization + "changepoint-detection_" + changepoint_detection + \
                                        "_oed_evaluation.tsv "
                        file_out_name_summary = method + "_words_freq-" + str(freq_filter) + "pvalue-" + str(
                            pvalue) + "_yearwindow-" + \
                                                str(
                                                    year_window) + "lemmatization-" + lemmatization + "changepoint-detection_" + \
                                                changepoint_detection + "_oed_evaluation_summary.txt"

                        # --------------------------------------------------------------
                        # Read word lists from different sources:
                        # --------------------------------------------------------------


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

                                if lemmatization == "yes":
                                    candidate2changepoints[lemmapos] = changepoints.split("_")
                                else:
                                    candidate2changepoints[lemma] = changepoints.split("_")
                                    # candidate2changepoints[lemma] = changepoints.split("_")

                        candidate_words_file.close()

                        # --------------------------------------------------------------
                        # Compare changepoint year of candidate words against OED years
                        # --------------------------------------------------------------

                        # Compare the corpus changepoints of a candidate lemma with its OED years:

                        correct_candidates = list()
                        candidates = list()
                        correct = 0

                        #correct_changepoints2overlap = dict() # maps a correct candidate and its correct changepoint to
                        # the number of overlapping words between its neighbours and the OED definition+quotation of
                        # its matching years


                        for l in candidate2changepoints:
                            changepoints = candidate2changepoints[l]
                            # print("Changepoints:", changepoints)
                            candidates.append(l)

                            if lemmatization == "yes":

                                if l in oed_lemmapos2years:
                                    print(l)
                                    oedyears = oed_lemmapos2years[l]
                                    # print("OED years:", oedyears)

                                    [correct, matching_oedyears] = compare_years(year_window, changepoints, oedyears)

                                    # correct candidates:

                                    if correct == 1:
                                        correct_candidates.append(l)

                                    # calculate overlap between corpus neighbours and OED definition+quotation:

                                    #for changepoint in changepoints:
                                    #    [overlap, oed_words, neigh_words, max_overlap] = calculate_overlap(
                                    #        lemmatization, l, changepoint, matching_oedyears, oedyears)


                            else:

                                if l in oed_lemma2years:
                                    print(l)
                                    oedyears = oed_lemma2years[l]
                                    # print("OED years:", oedyears)

                                    [correct, matching_oedyears] = compare_years(year_window, changepoints, oedyears)

                                    # correct candidates:

                                    if correct == 1:
                                        correct_candidates.append(l)

                                        # calculate overlap between corpus neighbours and OED definition+quotation:

                                        #for changepoint in changepoints:
                                        #    [overlap, oed_words, neigh_words, max_overlap] = calculate_overlap(
                                        #        lemmatization, l, changepoint, matching_oedyears, oedyears)

                        # Correct candidates:

                        correct_candidates = list(set(correct_candidates))

                        # --------------------------------------------------------------
                        # Calculate precision, recall, and F-score:
                        # --------------------------------------------------------------


                        print("Total number of correct candidates:", str(len(correct_candidates)))
                        print("Total number of candidates:", str(len(candidates)))
                        P = len(correct_candidates) / len(candidates)
                        print("Precision:", P)
                        print("Total number of gold standard lemmapos:", str(len(gold_standard_lemmapos)))
                        R = len(correct_candidates) / len(gold_standard_lemmapos)
                        print("Recall:", R)
                        if (P + R) > 0:
                            F = 2 * P * R / float(P + R)
                        else:
                            F = 0

                        # -----------------------------
                        # Print to output files:
                        # -----------------------------

                        # List of correct candidates, and their years:

                        output_file = open(os.path.join(dir_out, file_out_name), "w")

                        if lemmatization == "yes":
                            writer_output = csv.writer(output_file, delimiter='\t', quotechar='|',
                                                       quoting=csv.QUOTE_MINIMAL,
                                                       lineterminator='\n')
                            writer_output.writerow(
                                ['correct_lemma', 'correct_pos', 'changepoints', 'oed_years'])

                            for correct in correct_candidates:
                                lemma = correct.split("_")[0]
                                pos = correct.split("_")[1]
                                writer_output.writerow(
                                    [lemma, pos, candidate2changepoints[correct], oed_lemmapos2years[correct]])

                        else:
                            writer_output = csv.writer(output_file, delimiter='\t', quotechar='|',
                                                       quoting=csv.QUOTE_MINIMAL,
                                                       lineterminator='\n')
                            writer_output.writerow(['correct_lemma', 'changepoints', 'oed_years'])

                            for correct in correct_candidates:
                                lemma = correct
                                writer_output.writerow(
                                    [lemma, candidate2changepoints[correct], oed_lemmapos2years[correct]])

                        output_file.close()

                        # Evaluation statistics:

                        output_file_summmary = open(os.path.join(dir_out, file_out_name_summary), "w")
                        output_file_summmary.write(
                            "Total number of correct candidates:" + str(len(correct_candidates)) + "\n")
                        output_file_summmary.write("Total number of candidates:" + str(len(candidates)) + "\n")
                        output_file_summmary.write(
                            "Total number of gold standard lemmapos:" + str(len(gold_standard_lemmapos)) + "\n")
                        output_file_summmary.write("Precision:" + str(P) + "\n")
                        output_file_summmary.write("Recall:" + str(R))
                        output_file_summmary.close()

                        writer_all_output.writerow(
                            [method, changepoint_detection, pvalue, year_window, lemmatization,
                             str(len(correct_candidates)), str(len(candidates)), str(len(gold_standard_lemmapos)), P, R,
                             F])

# ---------------------------------------------------------------------
# Compare OED vector for candidate words with their corpus neighbours
# ---------------------------------------------------------------------


# r = requests.get('https://github.com/timeline.json')
# r.json()
