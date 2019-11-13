
#
# Take a SEMS output file and create a precinct tally report for contests belong to a particular section
#
# python -m converter.semsOutputToPrecintReport.py election.json <section_name> sems-output.csv > tally-report.csv
#
#
# SEMS format is CSV with fields:
# - county_id
# - precinct_id
# - contest_id
# - contest_title
# - party_id
# - party_label
# - candidate_id (or code 1 that indicates overvotes and code 2 that indicates blank votes)
# - candidate_name
# - candidate_party_id
# - candidate_party_label
# - count
#
# Approach: since the SEMS file already includes the data with exactly the model we need,
# this is simply a process of going through the file, saving the data we need, and outputting it
# in a different format. No need for sqlite here, as no data transforms.
#

import sys, json, csv

WRITEIN_CANDIDATE_ID = 0
OVERVOTE_CANDIDATE_ID = 1
UNDERVOTE_CANDIDATE_ID = 2

def create_precinct_report(election, section_name, sems_content_csv):
    contests = [c for c in election["contests"] if c["section"] == section_name]
    contest_ids = [c["id"] for c in contests]
    precincts = election["precincts"]

    # results indexed by precinct_id,contest_id,candidate_id
    results = {}
    for precinct in precincts:
        precinct_results = {}
        for contest in contests:
            precinct_results[contest["id"]] = {}
        results[precinct["id"]] = precinct_results
    
    for row in sems_content_csv:
        # windows ctrl-m issue --> extra empty row
        if len(row) == 0:
            continue

        county_id, precinct_id, contest_id, contest_title, party_id, party_label, candidate_id, candidate_name, candidate_party_id, candidate_party_label, count, trailing_empty = row

        if candidate_id == OVERVOTE_CANDIDATE_ID or candidate_id == UNDERVOTE_CANDIDATE_ID:
            continue

        if contest_id not in contest_ids:
            print(contest_id, contest_ids)
            continue
        
        results[precinct_id][contest_id][candidate_id] = int(count)

    return results

if __name__ == "__main__":
    election = json.loads(open(sys.argv[1],"r").read())
    section_name = sys.argv[2]
    sems_content = open(sys.argv[3], "r")
    sems_content_csv = csv.reader(sems_content, skipinitialspace=True)

    print(create_precinct_report(election, section_name, sems_content_csv))
