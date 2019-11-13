
#
# Creates a Precinct Tally Report for Specified Contests from Vx CVR files
#
# python createPrecinctReport.py election.json contest_id_1 cvrs.txt cvrs2.txt cvrs3.txt > output.csv
#

import csv, io, json, sqlite3, sys

NOPARTY_PARTY = {
    "id": "0",
    "name": "No Party",
    "abbrev": "NP"
}


YESNO_CANDIDATES = [
    ## TODO
]

CVR_FIELDS = ["county_id", "precinct_id", "contest_id", "candidate_id"]
CANDIDATE_FIELDS = ["county_id", "contest_id", "candidate_id"]

# which precincts do candidates appear in
CONTEST_PRECINCTS_FIELDS = ["contest_id", "precinct_id"]


def create_report(election_file_path, report_contest_id, cvr_path_list):
    election = json.loads(open(election_file_path,"r").read())
    contests = election["contests"]
    ballot_styles = election["ballotStyles"]
    precincts = election["precincts"]
    parties = election["parties"] + [NOPARTY_PARTY]

    county_id = election["county"]["id"]

    db = sqlite3.connect(":memory:")
    c = db.cursor()

    # create data model for contests and candidates so that we can track all candidates that are supposed to be reported on
    sql = "create table candidates (%s)" % ",".join(CANDIDATE_FIELDS)
    c.execute(sql)

    # create data model for candidate precinct mappings
    sql = "create table contest_precincts (%s)" % ",".join(CONTEST_PRECINCTS_FIELDS)
    c.execute(sql)


    def add_candidate(contest_id, candidate_id):
        value_placeholders = ["?"] * len(CANDIDATE_FIELDS)
        sql = "insert into candidates values (%s)" % ",".join(value_placeholders)
        c.execute(sql, [county_id, contest_id, candidate_id])

    def add_contest_precinct(contest_id, precinct_id):
        sql = "insert into contest_precincts values (?,?)"
        c.execute(sql, [contest_id, precinct_id])
    
    for contest in contests:
        # identify the ballot styles where this contest appears
        contest_ballot_styles = [bs for bs in ballot_styles if contest["districtId"] in bs["districts"]]
        contest_precincts = set()
        for bs in contest_ballot_styles:
            contest_precincts.update(bs["precincts"])

        contest_precincts = list(contest_precincts)

        for p in contest_precincts:
            add_contest_precinct(contest["id"], p)
    
        if contest["type"] == "yesno": # pragma: no cover
            ## TODO: implement yesno measures
            continue

        for candidate in contest["candidates"]:
            add_candidate(contest["id"], candidate["id"])


    # create data model for CVRs
    sql = "create table CVRs (%s)" % ",".join(CVR_FIELDS)
    c.execute(sql)

    def add_entry(precinct_id, contest_id, answer):
        value_placeholders = ["?"] * len(CVR_FIELDS)
        sql = "insert into CVRs values (%s)" % ",".join(value_placeholders)
        c.execute(sql, [county_id, precinct_id, contest_id, answer])

    contest = [c for c in contests if c["id"] == report_contest_id][0]
        
    # insert CVRs
    for cvr_path in cvr_path_list:
        cvrs_file = open(cvr_path, "r")
        while True:
            cvr = cvrs_file.readline()
            if not cvr:
                break

            cvr_obj = json.loads(cvr)
            precinct_id = cvr_obj["_precinctId"]

            answers = cvr_obj.get(contest["id"], None)

            if answers != None:
                # blank answer
                if answers == "":
                    continue
                
                # overvote, record only the overvote fact
                # TODO: consider removing this since there are no overvotes in our system
                if len(answers) > contest["seats"]: # pragma: no cover
                    continue
                
                for answer in answers:
                    if answer != 'writein':
                        add_entry(precinct_id, contest["id"], answer)

    # now it's all in in-memory sqlite
    report_sql = """
    select contest_precincts.precinct_id, candidates.candidate_id, CVRs.candidate_id, count(*) as count
    from candidates, contest_precincts
    join CVRs
    on
    candidates.contest_id = CVRs.contest_id and candidates.candidate_id = CVRs.candidate_id and contest_precincts.precinct_id = CVRs.precinct_id
    where
    candidates.contest_id = contest_precincts.contest_id
    and candidates.contest_id = ?
    group by contest_precincts.precinct_id, candidates.candidate_id, CVRs.candidate_id
    order by contest_precincts.precinct_id, candidates.candidate_id
    """

    # keyed by Precinct, then Candidate
    results = {}

    for row in c.execute(report_sql, [report_contest_id]).fetchall():
        precinct_id, candidate_id, CVR_candidate_id, count = row

        precinct = [p for p in precincts if p["id"] == precinct_id][0]
        candidate = [cand for cand in contest["candidates"] if cand["id"] == candidate_id][0]

        if precinct['id'] not in results:
            results[precinct['id']] = {}            
        precinct_results = results[precinct['id']]

        precinct_results[candidate['id']] = count

    results_table = []

    results_table.append(['', 'Total'] + [c['name'] for c in contest["candidates"]])

    for precinct in precincts:
        if precinct['id'] in results:
            results_table.append([
                precinct['name'],
                sum(results[precinct['id']].values())
            ] + [
                results[precinct['id']][candidate["id"]] for candidate in contest["candidates"]
            ])
    
    return results_table


if __name__ == "__main__":
    print(create_report(sys.argv[1], sys.argv[2], sys.argv[3:]))
