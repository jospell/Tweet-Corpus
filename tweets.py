from pyld import jsonld

import pprint
import glob
import pickle
import spacy


TWEETSFOLDER = "./tweetfiles/"
BINARYPATH = "./tweetsdict.p"
CORPUSPATH = "./tweetcorpus.txt"


def write_binary(data,path):
    '''
    Save binary data at path.
    '''

    # Rudimentary sanity check
    if data is None:
        print("Provided data is None, not saving to disk.")
        return

    # Write binary data
    print("Saving binary data at", path)
    with open(path, 'wb') as file:
        pickle.dump(data, file)


def read_binary(path):
    '''
    Read binary data from path.
    '''

    # Read Pickle
    print("Reading binary data from", path)
    with open(path, 'rb') as file:
        data = pickle.load(file)
        return data


def parse_tweet_file(filepath):
    '''
    Process contents of single tweet ldjson-file.
    '''

    null = None
    false = False
    true = True
    raw_data = None
    data = []

    with open(filepath, "r") as f:
        raw_data = f.readlines()
        for line in raw_data:
            data.append(eval(line))

    return data


def parse_all_tweet_files_in_folder(folderpath):
    '''
    Process all tweet-files at folderpath.
    '''

    current_file = 1
    tweetfilelist = glob.glob(folderpath + "*.*")
    tweets = {}

    for filename in tweetfilelist:
        print("Working on file", current_file, "of", len(tweetfilelist), "total files:", filename)
        current_file += 1
        tweets[filename] = parse_tweet_file(filename)

    return tweets

def get_four_from_all_files(tweets):
    '''
    Extract from the dictionary of all tweet-files the first 4 tweet-files.
    Used for debug-purposes.
    '''

    tweets_new = {}
    i = 0
    for key in tweets:
        i += 1
        if i <= 3:
            tweets_new[key] = tweets[key]
        else:
            break
    return tweets_new

def pos_tag_text(text, nlp):
    '''
    POS-tag text.
    '''

    tokens = None
    try:
        tokens = nlp(text)
    except:
        #print("Skipped text")
        pass

    return tokens # includes tok.pos_ tok.tag_

def create_corpus_costum_format(tweets):

    print("Writing Corpus...")

    nlp = spacy.load("en_core_web_sm") # nlp-module for POS-tagging

    with open(CORPUSPATH, "w") as file:
        file.write("<corpus name=\"Tweets\">")
        file.write("\n")

        total_tweet_cnt = 0
        skipped_text_count = 0
        for tweet_file in tweets: # files are structured as dictionary entries
            for file_line in tweets[tweet_file]: # file entries are structured in a list

                total_tweet_cnt += 1

                #print(file_line)
                user_screen_name = file_line["user"]["screen_name"]
                user_name = file_line["user"]["name"]
                user_location = file_line["user"]["location"]
                user_followers_count = file_line["user"]["followers_count"]
                user_verified = file_line["user"]["verified"]
                tweet_created_at = file_line["created_at"]
                tweet_retweet_count = file_line["retweet_count"]
                text = file_line["text"]
                tokens = pos_tag_text(text,nlp)
                if tokens is None:
                    skipped_text_count += 1
                    continue
                tagged = ""
                for t in tokens:
                    tagged = tagged + "\n" + str(t) + " " + str(t.tag_)
                #print(tagged)

                # write metadata header
                try:
                    file.write("<text user_screen_name=\"" + str(user_screen_name) + "\" user_name=\"" + \
                        str(user_name) + "\" user_location=\"" + str(user_location) + "\" user_followers_count=\"" + \
                        str(user_followers_count) + "\" user_verified=\"" + str(user_verified) + "\" tweet_created_at=\"" + \
                        str(tweet_created_at) + "\" tweet_retweet_count=\"" + str(tweet_retweet_count) + "\">")
                    file.write("\n")
                except:
                    skipped_text_count += 1
                    continue

                # write POS-tagged text
                file.write(tagged)
                file.write("\n")

                # write meta data header end tag
                file.write("<\\text>")
                file.write("\n")

        print("Total tweets:", total_tweet_cnt, "Skipped tweets:", skipped_text_count)

    print("Wrote corpus to disk.")

def create_corpus_se_format(tweets):

    print("Writing Corpus...")

    nlp = spacy.load("en_core_web_sm") # nlp-module for POS-tagging

    with open(CORPUSPATH, "w") as file:
        file.write("<xml>")
        file.write("\n")

        total_tweet_cnt = 0
        skipped_text_count = 0
        for tweet_file in tweets: # files are structured as dictionary entries
            for file_line in tweets[tweet_file]: # file entries are structured in a list

                total_tweet_cnt += 1

                if total_tweet_cnt % 100 == 0:
                    print("Processed", total_tweet_cnt, "tweets...")

                #print(file_line)
                user_screen_name = file_line["user"]["screen_name"]
                user_name = file_line["user"]["name"]
                user_location = file_line["user"]["location"]
                user_followers_count = file_line["user"]["followers_count"]
                user_verified = file_line["user"]["verified"]
                tweet_created_at = file_line["created_at"]
                tweet_retweet_count = file_line["retweet_count"]
                text = file_line["text"]
                tokens = pos_tag_text(text,nlp)
                if tokens is None:
                    skipped_text_count += 1
                    continue
                tagged = ""
                for t in tokens:
                    tagged = tagged + "\n" + str(t) + " " + str(t.tag_)
                #print(tagged)

                # write metadata header
                try:
                    file.write("<doc user_screen_name=\"" + str(user_screen_name) + "\" user_name=\"" + \
                        str(user_name) + "\" user_location=\"" + str(user_location) + "\" user_followers_count=\"" + \
                        str(user_followers_count) + "\" user_verified=\"" + str(user_verified) + "\" tweet_created_at=\"" + \
                        str(tweet_created_at) + "\" tweet_retweet_count=\"" + str(tweet_retweet_count) + "\">")
                    file.write("\n")
                except:
                    skipped_text_count += 1
                    continue

                # write POS-tagged text
                file.write("<p>")
                file.write(text)
                file.write("</p>\n")

                # end tag
                file.write("</doc>\n")

        file.write("</xml>\n")

        print("Total tweets:", total_tweet_cnt, "Skipped tweets:", skipped_text_count)

    print("Wrote corpus to disk.")

### MAIN

# read parsed tweets
tweets = None
try:
    tweets = read_binary(BINARYPATH)
    print("Read binary data from", BINARYPATH)
except:
    print("Found no binary data at path", BINARYPATH)
    print("Processing data from scratch.")
    tweets = parse_all_tweet_files_in_folder(TWEETSFOLDER)


print("Number of tweet-files:", len(tweets))

create_corpus_se_format(tweets)

# save tweets
#write_binary(tweets,BINARYPATH)
