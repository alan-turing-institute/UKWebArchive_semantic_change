## -*- coding: utf-8 -*-
# Author: Barbara McGillivray
# Date: 25/04/2018
# Python version: 3
# Script version: 1.0
# Script for evaluating the candidate words for semantic change outputted by the Temporal
# Random Indexing semantic change detection algorithm against OED words, and checking their corpus neighbours against
# the words occurring in the OED definitions and quotations.


# ----------------------------
# Initialization
# ----------------------------


# Import modules:

# import requests
import os
import csv
from nltk import word_tokenize
from nltk.corpus import stopwords
import datetime
import statistics

now = datetime.datetime.now()
today_date = str(now)[:10]
stop_words = set(stopwords.words('english'))

# Parameters:

freq_filter = 100  # frequency filter for candidate words
method_values = ["occ", "cum"]#["point", "occ", "cum"]
pvalue_values = ["095"]#["090", "095"]  # NB: if simple_valley_baseline = "yes", this parameter is ignored
freq_threshold = 500  # Frequency threshold for allowing a word into the corpus dictionary
changepoint_detection_values = ["simple_valley"]#["simple_valley", "mean_shift", "valley_var_1", "valley_var_2", "valley_var_4", "simple_valley", "mean_shift"]
year_window_values = ["greater"]#["greater", "2", "3"]  # this is the size of the window of years for matching changepoints to OED
# years;
# if "greater", then we match any changepoint that is >= OED year
lemmatization_values = ["no"]#["no", "yes"]
istest = input("Is this a test?")
testword = "red"
#testyear = 1998

# Directory and file names:

directory = os.path.join("/Users", "bmcgillivray", "Documents", "OneDrive", "The Alan Turing Institute",
                         "Visiting researcher Basile McGillivray - Documents")
dir_in = os.path.join(directory, "Evaluation", "OED")
dir_out = os.path.join(directory, "Evaluation", "output")

# create output directory if it doesn't exist:
if not os.path.exists(os.path.join(directory, "Evaluation", "output", str(today_date), "neighbours")):
    os.makedirs(os.path.join(directory, "Evaluation", "output", str(today_date), "neighbours"))
if not os.path.exists(os.path.join(directory, "Evaluation", "output", str(today_date), "precision-recall")):
    os.makedirs(os.path.join(directory, "Evaluation", "output", str(today_date), "precision-recall"))
if not os.path.exists(os.path.join(directory, "Evaluation", "output", str(today_date), "overview")):
    os.makedirs(os.path.join(directory, "Evaluation", "output", str(today_date), "overview"))

# Input files:
oed_senses_file_name = "oed.sense"
oed_quotations_file_name = "oed.quotation"
dict_file_name = "dict.sample.all"
neighbour_file_name = "neighborhood_dsPaper_100_plain.csv"#  "neighborhood_100.csv"

file_out_all_name = "oed_evaluation_overview"+str(today_date)+".csv"
if istest == "yes":
    file_out_all_name = file_out_all_name.replace(".csv", "_test.csv")
    method_values = ["cum"]
    pvalue_values = ["090"]
    changepoint_detection_values = ["simple_valley"]
    year_window_values = ["2"]
    lemmatization_values = ["no"]
    
# Read corpus neighbours:

neighbour_file = open(os.path.join(directory, "tri", "year", "v2", "neighborhood_100",
                                   neighbour_file_name), 'r')

row_count_neighbour = sum(1 for line in neighbour_file)
neighbour_file.close()

# read list of neighbours from corpus:

neighbour_file = open(os.path.join(directory, "tri", "year", "v2", "neighborhood_100",
                                   neighbour_file_name), 'r')
count_n = 0

neighbour_words = dict()  # maps a candidate and a year to the list of its top 100 corpus neighbours

for line in neighbour_file:
    count_n += 1
    #if istest == "yes":
    #    if count_n < 1000:
    #        #print("Neighbour dictionary: count", str(count_n), " out of ",
    #        #      str(row_count_neighbour))
    #        fields = line.split("\t")
    #        word = fields[0]
    #        year = int(fields[1])
    #        neighbours = fields[2:102]
    #        neighbour_words[(word, year)] = neighbours
    #        #print("neighbour_words of word year:", word, str(year), str(neighbours))
    #else:
    #    #print("Neighbour dictionary: count", str(count_n), " out of ",
    #    #      str(row_count_neighbour))
    fields = line.split("\t")
    word = fields[0]
    year = int(fields[1])
    neighbours = fields[2:102]
    neighbour_words[(word, year)] = neighbours

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
        #if istest == "yes":
        #    if count < 1000:
        #        #print("Corpus dictionary: count", str(count), " out of ", str(row_count))
        #        word = row[0]
        #        freq = int(row[1])
        #        if freq > freq_threshold:
        #            dict_words.append(word)
        #else:
        #    #print("Corpus dictionary: count", str(count), " out of ", str(row_count))
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
        #if istest == "yes":
        #    if count < 1000:
        #        #print("OED quotations: count", str(count), " out of ", str(row_count))
        #        # word = row[0]
        #        # lemma = row[1]
        #        oed_quotid = row[0]
        #        oed_senseid = row[1]
        #        oed_date = row[2]
        #        oed_quotation = row[3]
        #        oed_quotid2quot[oed_quotid] = oed_quotation
        
        #        if oed_senseid in oed_senseid2quotid:
        #            quotids = oed_senseid2quotid[oed_senseid]
        #            quotids.append(oed_quotid)
        #            oed_senseid2quotid[oed_senseid] = quotids
        #        else:
        #            oed_senseid2quotid[oed_senseid] = [oed_quotid]
        #else:
        #    #print("OED quotations: count", str(count), " out of ", str(row_count))
        #    # word = row[0]
        #    # lemma = row[1]
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
oed_lemmapos2senseid = dict()  # maps OED lemma_pos that is also candidate to its list of sense ids from OED
oed_lemmapos2senseid2year = dict()  # maps OED lemma_pos (that is also candidate) and sense id to the year of its first attestation
oed_lemma2senseid = dict()  # maps OED lemma that is also candidate to list of sense ids from OED
oed_lemma2senseid2year = dict()  # maps OED lemma that is also candidate and sense id to the year of its first attestation
oed_senseid2def = dict()  # maps OED sense id (of a lemma that is also candidate) to its definition
oed_lemmapos2years = dict()  # maps OED lemma_pos that is also candidate to list of years from OED
oed_lemmapos2year2senseid = dict()  # maps OED lemma_pos (that is also candidate) and year to the list of its sense ids
oed_lemma2years = dict()  # maps OED lemma that is also candidate to list of years from OED
oed_lemma2year2senseid = dict()  # maps OED lemma that is also candidate and year to the list of its sense ids

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

                if oed_lemmapos in oed_lemmapos2senseid:
                    senseids = oed_lemmapos2senseid[oed_lemmapos]
                    senseids.append(oed_senseid)
                    oed_lemmapos2senseid[oed_lemmapos] = senseids
                else:
                    oed_lemmapos2senseid[oed_lemmapos] = [oed_senseid]

                if (oed_lemmapos, oed_senseid) in oed_lemmapos2senseid2year.keys():
                    years = oed_lemmapos2senseid2year[(oed_lemmapos, oed_senseid)]
                    years.append(oed_senseid)
                    oed_lemmapos2senseid2year[(oed_lemmapos, oed_senseid)] = years
                else:
                    oed_lemmapos2senseid2year[(oed_lemmapos, oed_senseid)] = [oed_startdate]

                # OED lemmas:

                gold_standard_lemma.append(oed_lemma)

                if oed_lemma in oed_lemma2senseid:
                    senseids = oed_lemma2senseid[oed_lemma]
                    # years = oed_lemmapos2years[oed_lemma]
                    senseids.append(oed_senseid)
                    oed_lemma2senseid[oed_lemma] = senseids
                    # oed_lemmapos2years[oed_lemma] = years
                else:
                    oed_lemma2senseid[oed_lemma] = [oed_senseid]
                    # oed_lemmapos2years[oed_lemma] = [oed_startdate]


                oed_lemma2senseid2year[(oed_lemma, oed_senseid)] = oed_startdate

                if oed_lemma in oed_lemma2years:
                    years = oed_lemma2years[oed_lemma]
                    # years = oed_lemmapos2years[oed_lemma]
                    years.append(oed_startdate)
                    oed_lemma2years[oed_lemma] = years
                    # oed_lemmapos2years[oed_lemma] = years
                else:
                    oed_lemma2years[oed_lemma] = [oed_startdate]
                    # oed_lemmapos2years[oed_lemma] = [oed_startdate]

                if oed_lemmapos in oed_lemmapos2years:
                    years = oed_lemmapos2years[oed_lemmapos]
                    # years = oed_lemmapos2years[oed_lemma]
                    years.append(oed_startdate)
                    oed_lemmapos2years[oed_lemmapos] = years
                    # oed_lemmapos2years[oed_lemma] = years
                else:
                    oed_lemmapos2years[oed_lemmapos] = [oed_startdate]
                    # oed_lemmapos2years[oed_lemma] = [oed_startdate]

oed_senses_file.close()


# Function that compares a list of change points from the corpus with a list of years from OED:

def compare_years(year_window_par, changepoints_list, oedyears_list):

    print("----- Compare years.....")
    is_correct_var = 0
    correct_changepoints_list = list()  # list of changepoints for which there is at least a matching OED year
    # (there could be repetitions)
    matching_oedyears_list = list()  # for every correct changepoint, the matching OED year

    for ch in changepoints_list:
        print("\tChangepoint:" + str(ch))
        for oedyear in oedyears_list:
            print("\t\tOED year:" + str(oedyear))
            # if int(changepoint) == int(oedyear) or int(changepoint) == int(oedyear) - 1
            # or int(changepoint) == int(oedyear) + 1:
            if year_window_par == "greater":
                if int(ch) >= int(oedyear) >= 1995:
                    print("\t\t\tCorrect changepoint (greater)", ": ", str(ch), str(oedyear))
                    is_correct_var = 1
                    correct_changepoints_list.append(ch)
                    matching_oedyears_list.append(oedyear)
            else:
                if ((int(oedyear) - int(year_window_par) <= int(ch) <= int(oedyear) + int(
                        year_window_par)) and (int(oedyear) >= 1995)):
                    print("\t\t\tCorrect changepoint (window)", ": ", str(ch), str(oedyear))
                    is_correct_var = 1
                    correct_changepoints_list.append(ch)
                    matching_oedyears_list.append(oedyear)

    return [is_correct_var, correct_changepoints_list, matching_oedyears_list]

# Function to calculate the Jaccard index betweent two sets
# (from https://stackoverflow.com/questions/49255121/jaccard-index-python):

def jaccard_index(first_set, second_set):
    """ Computes jaccard index of two sets
        Arguments:
          first_set(set):
          second_set(set):
        Returns:
          index(float): Jaccard index between two sets; it is
            between 0.0 and 1.0
    """
    # If both sets are empty, jaccard index is defined to be 1
    index = 1.0
    if first_set or second_set:
        index = (float(len(first_set.intersection(second_set)))
                 / len(first_set.union(second_set)))

    return index

# Function that calculates the overlap between corpus neighbours and OED definition+quotation for a given matching year:

def calculate_overlap(lemmatization_par, candidate, changepoint_year, matching_oedyear):

    print("-------Calculate overlap....")
    changepoint_year = int(changepoint_year)
    # and preprocessed OED definition+quotation words at matching year
    senseid2jaccard_dict = dict()  # maps a sense id to the Jaccard similarity score between corpus neighbours of
    # l at changeppoint_year and preprocessed OED definition+quotation words in that sense
    senseid2overlap_dict = dict()  # maps a sense id to the list of words overlapping between corpus neighbours
    # of l at changepoint_year and pre-processed OED definition+quotation words in that sense
    senseid2jaccard_ranked_dict = dict()  # year2jaccard, ranked by decreasing value
    # max_overlap = 0  # maximum among number of words shared between corpus neighbours of l at changeppoint_year
    # and preprocessed OED definition+quotation words in any year >= 1995
    rank_var = "NA"  # rank of the candidate's neighbours in the list of OED content ordered by decreasing Jaccard
    # similarity
    overlap_var = "NA"  # overlap between the candidate's neighbours and the list of OED content of the corresponding entry
    matching_year_jaccard = "NA"  # Jaccard index of the candidate's neighbours with the OED sense of the matching year
    #print("Matching OED year:", matching_oedyear)
    #print("Candidate:", candidate)
    #print("Changepoint year:", changepoint_year)
    #print("Lemmatization:", lemmatization_par)

    if lemmatization_par == "yes":
        lemm = candidate.split("_")[0]
    else:
        lemm = candidate

    #print("Lemm:", lemm)

    # retrieve definitions of lemma (or lemma_pos) in OED for each of the years and for the matching years:

    oedsenseid2content_words = dict()  # maps a sense id to the list of words in each OED definitions+quotations

    #oed_years_candidate = list()  # list of years associated to candidate in OED
    if lemmatization_par == "yes":
        oed_senseids_candidate = oed_lemmapos2senseid[candidate]
    else:
        oed_senseids_candidate = oed_lemma2senseid[candidate]

    print("oed sense ids candidate:", oed_senseids_candidate)

    for senseid in oed_senseids_candidate:
        print("\tOED sense id:", str(senseid))

        oed_words_list = list()

        #print("OED id:", oed_sid)
        oed_def = oed_senseid2def[senseid]
        oed_definition_words = word_tokenize(oed_def)
        #print("OED def:", oed_def, str(oed_definition_words))

        for w in oed_definition_words:
            w = w.lower()
            if w not in stop_words:
                oed_words_list.append(w)
                if istest == "yes" and lemm == testword:
                    print("Word from OED definition ", str(senseid), ":", w)
                #if oedy == matching_oedyear:
                #    oed_content_words_matching_year.append(w)

        # retrieve quotations of this sense id in OED:

        if senseid in oed_senseid2quotid:
            quot_ids = oed_senseid2quotid[senseid]
            for quotid in quot_ids:
                if quotid in oed_quotid2quot:
                    quotation = oed_quotid2quot[quotid]
                    oed_quotation_words = word_tokenize(quotation)
                    for w in oed_quotation_words:
                        w = w.lower()
                        if w not in stop_words:
                            oed_words_list.append(w)
                        if istest == "yes" and lemm == testword:
                            print("Word from OED quotation ", str(quotid), ":", w)


                                #if oedy == matching_oedyear:
                        #    oed_content_words_matching_year.append(w)

        if lemm in oed_words_list:
            oed_words_list.remove(lemm)
        else:
            pass

        oedsenseid2content_words[senseid] = list(set(oed_words_list))

    #oed_content_words_matching_year = list(set(oed_content_words_matching_year))
    #print("oed_content_words_matching_year:", str(oed_content_words_matching_year))
    #print("oed_content_words_all_years:", oed_content_words_all_years)

    # Calculate the Jaccard similarity between the corpus neighbours of the candidate and the OED definition+quotation
    # words for each year, and rank them in descending order:

    #print("neighbour_words:", str(neighbour_words))
    #print("keys:", neighbour_words.keys())
    #print(str(type(changepoint_year)))
    # if any(k[0] == lemm for k in neighbour_words):
    #     kk = [k for k in neighbour_words if k[0] == lemm]
    #     print("yes!!")
    #     print(str(kk))
    #     print(str(kk[0][1]))
    #     print(str(type(kk[0][1])))n
    #     if str(kk[0][1]) == str(changepoint_year):
    #         print("same as ", str(changepoint_year))
    #     print(str(kk[1][1]))
    #     print(str(type(kk[1][1])))
    #     if str(kk[1][1]) == str(changepoint_year):
    #         print("same as ", str(changepoint_year))
    #
    #     if any(x[1] == changepoint_year for x in kk):
    #         print("yes year")
    #         kkk = [x[1] == changepoint_year for x in kk]
    #         print(str(kkk))

    if (lemm, changepoint_year) in neighbour_words.keys():
        neighbour_ws = neighbour_words[(lemm, changepoint_year)]

        neighbour_ws = list(set(neighbour_ws))
        if lemm in neighbour_ws:
            neighbour_ws.remove(lemm)
        #print("neighbour words for ", lemm, ": ", neighbour_ws)

        for senseid in oed_senseids_candidate:
            #print("oedy:", str(oedy))
            #print("oed_content_words_all_years[oedy]:", str(oed_content_words_all_years[oedy]))
            #print("this was a ", str(type(oed_content_words_all_years[oedy])))
            #print("neighbour words for ", lemm, ": ", neighbour_ws)
            #print("this was a ", str(type(neighbour_ws)))
            #year2jaccard_dict[oedy] = jaccard_similarity_score(neighbour_ws, oed_content_words_all_years[oedy])
            senseid2jaccard_dict[senseid] = jaccard_index(set(neighbour_ws), set(oedsenseid2content_words[senseid]))
            senseid2overlap_dict[senseid] = set(neighbour_ws).intersection(set(oedsenseid2content_words[senseid]))
            print("Overlapping words for ", str(senseid), ": ", str(senseid2jaccard_dict[senseid]),
                  #"\nOverlap between ",
                  #str(sorted(neighbour_ws)), "\n\t and ", str(sorted(oedsenseid2content_words[senseid])), ": \n",
                  str(senseid2overlap_dict[senseid]))
            #print("oed year, jaccard:", str(oedy), str(year2jaccard_dict[oedy]))

        senseid2jaccard_ranked_dict = sorted(senseid2jaccard_dict.items(), key=lambda kv: kv[1], reverse=True)
        print("senseid2jaccard_ranked_dict:", str(senseid2jaccard_ranked_dict))

        # check where the new detected sense ranks among the OED senses:

        count_y = 0
        for s in senseid2jaccard_ranked_dict:
            sid = s[0]
            count_y += 1
            print("sense id:", str(sid))

            if lemmatization == "yes":
                oedy = oed_lemmapos2senseid2year[(candidate, sid)]
            else:
                oedy = oed_lemma2senseid2year[(candidate, sid)]

            print("Oedy:", str(oedy), ", matching_oedyear:", str(matching_oedyear))
            if str(oedy) == str(matching_oedyear):
                rank_var = count_y
                matching_year_jaccard = senseid2jaccard_dict[sid]
                overlap_var = senseid2overlap_dict[sid]
                print("\trank of matching year:", str(rank_var))

    # return outputs:

    return [rank_var, matching_year_jaccard, senseid2jaccard_ranked_dict, overlap_var]

#csvfile = open(os.path.join(dir_out, str(today_date), "overview", "test.csv"), "w")
#writer = csv.writer(csvfile)
#for filename in os.listdir('/'): # or C:\\ if on Windows
#    writer.writerow([filename, len(filename)])
#csvfile.close()

# -----------------------------------------
# Write to output
# -----------------------------------------

with open(os.path.join(dir_out, str(today_date), "overview", file_out_all_name), "w") as output_all_file:
    writer_all_output = csv.writer(output_all_file, delimiter='\t', quotechar='|', quoting=csv.QUOTE_MINIMAL,
                                   lineterminator='\n')
    writer_all_output.writerow(['method', 'changepoint detection', 'significance', 'year_window', 'lemmatization',
                                'num correct', 'num candidates', 'num gold standard', 'Precision', 'Recall',
                                'F1-score', 'average_rank', 'average_Jaccard'])

    for lemmatization in lemmatization_values:

        for changepoint_detection in changepoint_detection_values:
            pvalue_values = ["090", "095"]
            #if changepoint_detection.startswith("valley_var"):
            #    pvalue_values = ["0"]  # NB: if simple_valley_baseline = "yes", this parameter is ignored

            for method in method_values:

                if changepoint_detection.startswith("valley_var"):
                    pvalue_values = ["0"]
                else:
                    pvalue_values = ["090", "095"]
                if istest == "yes":
                    pvalue_values = ["090"]

                for pvalue in pvalue_values:

                    for year_window in year_window_values:

                        candidate_words_file_name = method + "_words_for_lookup_freq_" + str(freq_filter) + \
                                                    "pvalue-" + str(pvalue) + "changepoint-detection_" + \
                                                    changepoint_detection + ".txt"

                        # Output files: candidate words for semantic change detection, checked against OED API:

                        file_out_name = method + '_words_freq-' + str(freq_filter) + "pvalue-" + str(
                            pvalue) + "_yearwindow-" + str(year_window) + "lemmatization-" + lemmatization + \
                                        "changepoint-detection_" + changepoint_detection + "_oed_evaluation.tsv"
                        file_out_name_summary = method + "_words_freq-" + str(freq_filter) + "pvalue-" + str(pvalue) + \
                                                "_yearwindow-" + str(year_window) + "lemmatization-" + lemmatization + \
                                                "changepoint-detection_" + changepoint_detection + \
                                                "_oed_evaluation_summary.txt"
                        file_out_name_summary_neigh = method + "_words_freq-" + str(freq_filter) + "pvalue-" + str(pvalue) + \
                                                "_yearwindow-" + str(year_window) + "lemmatization-" + lemmatization + \
                                                "changepoint-detection_" + changepoint_detection + \
                                                "_neighbours_summary.txt"
                        file_out_neigh_name = method + '_words_freq-' + str(freq_filter) + "pvalue-" + str(
                            pvalue) + "_yearwindow-" + str(year_window) + "lemmatization-" + lemmatization + \
                                              "changepoint-detection_" + changepoint_detection + "_oed_neighbour_evaluation.tsv"
                        if istest == "yes":
                            file_out_neigh_name = file_out_neigh_name.replace(".tsv", "_test.tsv")
                            file_out_name_summary_neigh = file_out_name_summary_neigh.replace(".txt", "_test.txt")
                            file_out_name_summary = file_out_name_summary.replace(".txt", "_test.txt")

                        neigh_output_file = open(os.path.join(dir_out, str(today_date), "neighbours", file_out_neigh_name), "w")

                        if lemmatization == "yes":
                            writer_neigh_output = csv.writer(neigh_output_file, delimiter='\t', quotechar='|',
                                                             quoting=csv.QUOTE_MINIMAL,
                                                             lineterminator='\n')
                            writer_neigh_output.writerow(
                                ['candidate', 'pos', 'changepoint', 'oed_year', 'jaccard_similarity', 'rank', 'num_senses', 'overlap'])

                        else:
                            writer_neigh_output = csv.writer(neigh_output_file, delimiter='\t', quotechar='|',
                                                             quoting=csv.QUOTE_MINIMAL,
                                                             lineterminator='\n')
                            writer_neigh_output.writerow(
                                ['candidate', 'changepoint', 'oed_year', 'jaccard_similarity', 'rank', 'num_senses', 'overlap'])

                        # --------------------------------------------------------------
                        # Read word lists from different sources:
                        # --------------------------------------------------------------


                        # Read corpus changepoints for candidate words:

                        print("Reading candidates from ", str(os.path.join(dir_out, candidate_words_file_name)))
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

                                if lemmatization == "yes":
                                    if istest == "yes":
                                        if lemmapos.startswith(testword):
                                            candidate2changepoints[lemmapos] = changepoints.split("_")
                                        else:
                                            pass
                                    else:
                                        candidate2changepoints[lemmapos] = changepoints.split("_")
                                else:
                                    if istest == "yes":
                                        if lemma == testword:
                                            candidate2changepoints[lemma] = changepoints.split("_")
                                        else:
                                            pass
                                    else:
                                        candidate2changepoints[lemma] = changepoints.split("_")
                                    # candidate2changepoints[lemma] = changepoints.split("_")
                            else:
                                pass

                        candidate_words_file.close()

                        # --------------------------------------------------------------
                        # Compare changepoint year of candidate words against OED years
                        # --------------------------------------------------------------

                        # Compare the corpus changepoints of a candidate lemma with its OED years:

                        correct_candidates = list()
                        candidates = list()
                        correct = 0
                        ranks = list()  # lists all ranks of candidates in OED senses
                        jaccards = list()  # lists all Jaccard indices of candidates in OED senses

                        # correct_changepoints2overlap = dict() # maps a correct candidate and its correct changepoint to
                        # the number of overlapping words between its neighbours and the OED definition+quotation of
                        # its matching years

                        for l in candidate2changepoints:

                            changepoints = candidate2changepoints[l]
                            # print("Changepoints:", changepoints)
                            candidates.append(l)

                            if lemmatization == "yes":
                                candidate_lemma = l.split("_")[0]
                                candidate_pos = l.split("_")[1]
                                #writer_all_output.writerow(["1"])

                                if l in oed_lemmapos2years:
                                    oedyears = oed_lemmapos2years[l]
                                    print("OED years:", oedyears)

                                    [is_correct, correct_changepoints, matching_oedyears] = \
                                        compare_years(year_window, changepoints, oedyears)

                                    # correct candidates:

                                    if is_correct == 1:
                                        correct_candidates.append(l)

                                        # calculate overlap between corpus neighbours and OED definition+quotation:

                                        for ic in range(len(correct_changepoints)):
                                            changepoint = correct_changepoints[ic]
                                            matching_oedyear = matching_oedyears[ic]
                                            print("\tMatching oedyear:", str(matching_oedyear))
                                            print("\tCalculating overlap for ", l, " in ", str(changepoint))
                                            [rank, jaccard, senseid2jaccard_ranked, overlap] = \
                                                     calculate_overlap(lemmatization, l, changepoint, matching_oedyear)
                                            print("\t\tRank ", str(rank))

                                            if rank != "NA":
                                                ranks.append(rank)
                                                print("Ranks:", str(ranks))
                                            if jaccard != "NA":
                                                jaccards.append(jaccard)

                                            #for k in senseid2jaccard_ranked:
                                            #    sid = k[0]  # the OED sense id
                                            #    jacc = k[1]  # Jaccard similarity between OED sense id and candidate's neighbours

                                                    # write to neighbour output file:
                                            print("Output neighbours:", candidate_lemma, candidate_pos,
                                                                                    str(changepoint),
                                                            str(matching_oedyear), str(jaccard), str(rank))
                                            writer_neigh_output.writerow([candidate_lemma, candidate_pos,
                                                                                    changepoint, matching_oedyear,
                                                                                    jaccard, rank,str(len(senseid2jaccard_ranked)),
                                                                                  '-'.join(overlap)])
                                    else:
                                        pass
                                else:
                                    pass

                            else:

                                if l in oed_lemma2years:
                                    print("Lemma:", l)
                                    oedyears = oed_lemma2years[l]
                                    print("OED years:", oedyears)

                                    [is_correct, correct_changepoints, matching_oedyears] = \
                                        compare_years(year_window, changepoints, oedyears)

                                    # correct candidates:

                                    if is_correct == 1:
                                        correct_candidates.append(l)

                                        # calculate overlap between corpus neighbours and OED definition+quotation:
                                        print("Changepoints:", str(changepoints), "matching years:", str(matching_oedyears))
                                        for ic in range(len(correct_changepoints)):
                                            changepoint = correct_changepoints[ic]
                                            matching_oedyear = matching_oedyears[ic]
                                            print("\n\nCalculating overlap for ", l, "at changepoint", str(changepoint),
                                                  "and matching oedyear", str(matching_oedyear))
                                                
                                            [rank, jaccard, senseid2jaccard_ranked, overlap] = \
                                                    calculate_overlap(lemmatization, l, changepoint, matching_oedyear)
                                            print("\t\tRank ", str(rank))

                                            if rank != "NA":
                                                ranks.append(rank)
                                                print("Ranks:", str(ranks))
                                            if jaccard != "NA":
                                                jaccards.append(jaccard)

                                            #for k in senseid2jaccard_ranked:
                                            #    sid = k[0]  # the OED year
                                            #    jacc = k[1]  # Jaccard similarity between oedy and candidate's neighbours

                                                # write to neighbour output file:
                                            print("\t\t\tOutput neighbours:", l, str(changepoint),
                                                        str(matching_oedyear), str(jaccard), str(rank))
                                            writer_neigh_output.writerow([l, changepoint, matching_oedyear,
                                                                                  jaccard, rank,str(len(senseid2jaccard_ranked)),
                                                                                  '-'.join(overlap)])
                                    else:
                                        pass
                                else:
                                    pass

                        neigh_output_file.close()

                        # Correct candidates:

                        correct_candidates = list(set(correct_candidates))

                        # --------------------------------------------------------------
                        # Calculate precision, recall, and F-score:
                        # --------------------------------------------------------------


                        print("Total number of correct candidates:", str(len(correct_candidates)))
                        print("Total number of candidates:", str(len(candidates)))
                        P = 0
                        if len(candidates) > 0:
                            P = len(correct_candidates) / len(candidates)
                        print("Precision:", P)
                        print("Total number of gold standard lemmapos:", str(len(gold_standard_lemmapos)))
                        R = len(correct_candidates) / len(gold_standard_lemmapos)
                        print("Recall:", R)
                        if (P + R) > 0:
                            F = 2 * P * R / float(P + R)
                        else:
                            F = 0

                        # -------------------------------------
                        # Calculate average rank of candidates
                        # -------------------------------------

                        av_rank = "NA"
                        if len(ranks)>0:
                            av_rank = sum(ranks)/len(ranks)
                        print("Mean of ranks:", str(av_rank))
                        med_rank = "NA"
                        if len(ranks) > 0:
                            med_rank = statistics.median(ranks)
                        print("Median of ranks:", str(med_rank))

                        # ------------------------------------------
                        # Calculate average rank of Jaccard indices
                        # ------------------------------------------

                        av_jaccard = "NA"
                        if len(jaccards) > 0:
                            av_jaccard = sum(jaccards)/len(jaccards)#statistics.mean(jaccards)
                        print("Mean of Jaccard indices:", str(av_jaccard))

                        # -----------------------------
                        # Print to output files:
                        # -----------------------------

                        # List of correct candidates, and their years:

                        with open(os.path.join(dir_out, str(today_date), "precision-recall", file_out_name), "w") as output_file:

                            if lemmatization == "yes":
                                writer_output = csv.writer(output_file, delimiter='\t', quotechar='|',
                                                           quoting=csv.QUOTE_MINIMAL,
                                                           lineterminator='\n')
                                writer_output.writerow(
                                    ['correct_lemma', 'correct_pos', 'changepoints', 'oed_years'])

                                for c in correct_candidates:
                                    lemma = c.split("_")[0]
                                    pos = c.split("_")[1]
                                    writer_output.writerow(
                                        [lemma, pos, candidate2changepoints[c], oed_lemmapos2years[c]])
                                    print("Output:", lemma, pos, str(candidate2changepoints[c]),
                                          str(oed_lemmapos2years[c]))

                            else:
                                writer_output = csv.writer(output_file, delimiter='\t', quotechar='|',
                                                           quoting=csv.QUOTE_MINIMAL,
                                                           lineterminator='\n')
                                writer_output.writerow(['correct_lemma', 'changepoints', 'oed_years'])

                                for c in correct_candidates:
                                    lemma = c
                                    if c in candidate2changepoints and c in oed_lemma2years:
                                        writer_output.writerow(
                                            [lemma, candidate2changepoints[c], oed_lemma2years[c]])
                                        print("Output:", lemma, str(candidate2changepoints[c]), str(oed_lemma2years[c]))

                        # Evaluation statistics:

                        output_file_summmary = open(os.path.join(dir_out, str(today_date), "precision-recall", file_out_name_summary), "w")
                        output_file_summmary.write(
                            "Total number of correct candidates:" + str(len(correct_candidates)) + "\n")
                        output_file_summmary.write("Total number of candidates:" + str(len(candidates)) + "\n")
                        output_file_summmary.write(
                            "Total number of gold standard lemmapos:" + str(len(gold_standard_lemmapos)) + "\n")
                        output_file_summmary.write("Precision:" + str(P) + "\n")
                        output_file_summmary.write("Recall:" + str(R))
                        output_file_summmary.close()

                        # Neighbour statistics:
                        output_file_summmary_neigh = open(
                            os.path.join(dir_out, str(today_date), "neighbours", file_out_name_summary_neigh), "w")
                        output_file_summmary_neigh.write(
                            "Total number of ranks:" + str(len(ranks)) + "\n")
                        output_file_summmary_neigh.write(
                            "List of ranks:" + ','.join(str(ranks)) + "\n")
                        output_file_summmary_neigh.write("Average rank:" + str(av_rank) + "\n")
                        output_file_summmary_neigh.write(
                            "Median rank:" + str(med_rank) + "\n")
                        output_file_summmary_neigh.close()

                        #writer_all_output.writerow([1,2,3,4])
                        writer_all_output.writerow(
                            [method, changepoint_detection, pvalue, year_window, lemmatization,
                             str(len(correct_candidates)), str(len(candidates)), str(len(gold_standard_lemmapos)), P, R,
                             F, av_rank, av_jaccard])
                        print(method, changepoint_detection, pvalue, str(year_window), lemmatization,
                              str(len(correct_candidates)), str(len(candidates)), str(len(gold_standard_lemmapos)),
                              str(P), str(R), str(F), str(av_rank), str(av_jaccard))

