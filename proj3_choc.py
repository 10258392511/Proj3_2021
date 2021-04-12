######################
# Name: Zhexin Wu
# Uniquename: zhexinwu
######################


import sqlite3
import re
import plotly.graph_objects as go
from collections import defaultdict


# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from a database called choc.db
DBNAME = 'choc.sqlite'


# Part 1: Implement logic to process user commands
def process_command(command):
    """
    Take a str user input, return a list of records.

    Parameters
    ----------
    command: str
        Raw user input.

    Returns
    -------
    list
        List of records as tuples.
    """
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    try:
        parsed_dict = extract_and_group_commands(command)
    except InvalidInputError as e:
        # print(e)
        # return []
        raise e

    query = None
    # print(f"parsed_dict: {parsed_dict}")
    high_level = parsed_dict["high_level"]
    try:
        if high_level == "bars":
            query = query_bars(parsed_dict, command)
        elif high_level == "companies":
            query = query_companies(parsed_dict, command)
        elif high_level == "countries":
            query = query_countries(parsed_dict, command)
        elif high_level == "regions":
            query = query_regions(parsed_dict, command)
    except InvalidInputError as e:
        # print(e)
        # return []
        raise e

    cur.execute(query)
    results = list(cur.fetchall())
    conn.close()

    return results


class InvalidInputError(Exception):
    """
    A custom exception for invalid user input.
    """
    def __init__(self, msg="Invalid user input"):
        super().__init__(msg)


def extract_and_group_commands(user_in):
    """
    Helper function to check and extract high-level command.

    Parameters
    ----------
    user_in: str
        Raw user input.

    Returns
    -------
    dict or raise InvalidInputError
        If the user input is invalid, an InvalidInputError Exception occurs.
        Otherwise it's the parsed results as a dict like
        {"high_level": str,
        "groups": list[str],
        "is_user_input": list[int] (-1: no user input, others: user input),
        "barplot": bool}
    """
    user_in = user_in.strip()
    parsed_syms = user_in.split(" ")
    processed_inds = []
    error_msg = f"Command not recognized: {user_in}"
    if len(user_in) == 0:
        # return None
        raise InvalidInputError(error_msg)

    # extract high-level command
    high_level_syms = ["bars", "companies", "countries", "regions"]
    high_level_ind = extract_args(high_level_syms, parsed_syms)
    if high_level_ind == -2 or high_level_ind > 0:  # multiple or not the first one
        raise InvalidInputError(error_msg)
    elif high_level_ind == -1:
        high_level = "bars"
    else:
        processed_inds.append(high_level_ind)
        high_level = parsed_syms[0]

    # print(f"High level: {high_level}")

    # extract group 1 params
    pattern = re.compile(r"(country|region)=.*")
    group1_ind = extract_kwargs(pattern, parsed_syms)
    if group1_ind == -2:
        # return None
        raise InvalidInputError(error_msg)

    elif group1_ind == -1:
        group1 = None
    else:
        group1 = parsed_syms[group1_ind]
        processed_inds.append(group1_ind)
    # print(f"group 1: {group1}")

    # extract group 2 params
    group2_ind = extract_args(["sell", "source"], parsed_syms)
    if group2_ind == -2:
        # return None
        raise InvalidInputError(error_msg)

    elif group2_ind == -1:
        group2 = "sell"
    else:
        group2 = parsed_syms[group2_ind]
        processed_inds.append(group2_ind)

    # extract group 3 params
    group3_ind = extract_args(["ratings", "cocoa", "number_of_bars"], parsed_syms)
    if group3_ind == -2:
        # return None
        raise InvalidInputError(error_msg)

    elif group3_ind == -1:
        group3 = "ratings"
    else:
        group3 = parsed_syms[group3_ind]
        processed_inds.append(group3_ind)

    # extract group 4 params
    group4_ind = extract_args(["top", "bottom"], parsed_syms)
    if group4_ind == -2:
        # return None
        raise InvalidInputError(error_msg)

    elif group4_ind == -1:
        group4 = "top"
    else:
        group4 = parsed_syms[group4_ind]
        processed_inds.append(group4_ind)

    # extract group 5 params
    # can utilize extract_kwargs(.)
    group5_ind = extract_kwargs(re.compile(r"^[0-9]*$"), parsed_syms)
    if group5_ind == -2:
        # return None
        raise InvalidInputError(error_msg)
    elif group5_ind == -1:
        group5 = 10
    else:
        group5 = int(parsed_syms[group5_ind])
        processed_inds.append(group5_ind)

    # extract barplot param
    group6_ind = extract_args(["barplot"], parsed_syms)
    if group6_ind == -2:
        # return None
        raise InvalidInputError(error_msg)
    elif group6_ind == -1:
        group6 = False
    else:
        # "barplot" must be supplied last
        if group6_ind != len(parsed_syms) - 1:
            raise InvalidInputError(error_msg)
        group6 = True
        processed_inds.append(group6_ind)

    # finally there should be no unprocessed parts of the user input
    if not len(processed_inds) == len(parsed_syms):
        # return None
        raise InvalidInputError(error_msg)

    # return the dict
    parsed_dict = {"high_level": high_level,
                   "groups": [group1, group2, group3, group4, group5],
                   "is_user_input": [group1_ind, group2_ind, group3_ind, group4_ind, group5_ind],
                   "barplot": group6}

    return parsed_dict


def extract_kwargs(kwargs_pattern, syms):
    """
    Helper function for extract_and_group_commands(.). Extracts keyword arguments.

    Parameters
    ----------
    kwargs_pattern: re.Pattern
        A pattern like "(country|region)=*".
    syms: list
        List of parsed symbols.

    Returns
    -------
    group_ind: int
        If -2, the user input is invalid; otherwise, the element is either -1 meaning using default and
        with no match in "syms"; or an index of "syms".
    """
    has_group = False
    group_ind = -1
    for i, sym in enumerate(syms):
        if kwargs_pattern.match(sym):
            if has_group:  # multiple ones: invalid
                return -2
            group_ind = i
            has_group = True

    return group_ind


def extract_args(args, syms):
    """
    Helper function for extract_and_group_commands(.). Extracts positional arguments.

    Parameters
    ----------
    args: list
        A list of valid str symbols.
    syms: list
        List of parsed symbols.

    Returns
    -------
    group_ind: int
        If -2, the user input is invalid; otherwise, the element is either -1 meaning using default and
        with no match in "syms"; or an index of "syms".
    """
    has_group = False
    group_ind = -1
    for i, sym in enumerate(syms):
        if sym in args:
            if has_group:  # multiple ones: invalid
                return -2
            group_ind = i
            has_group = True

    return group_ind


def query_bars(parsed_dict, cmd):
    """
    Using a dict representing parsed command from extract_and_group_commands(.),
    validate parameters and construct SQL for high-level command "bars".
    Note a user can add parameters in arbitrary order.

    Parameters
    ----------
    parsed_dict: dict
        A list of parsed symbols.
    cmd: str
        Original command used for error message.

    Returns
    -------
    query: str
        The appropriate SQL for the user input.
    """
    assert parsed_dict["high_level"] == "bars", "wrong function used"
    error_msg = f"Command not recognized (invalid selection of parameters): {cmd}"
    if parsed_dict["groups"][2] == "number_of_bars":  # if "number_of_bars" exists, it must be user input
        raise InvalidInputError(error_msg)

    query = """
    SELECT SpecificBeanBarName, Company, C_companies.EnglishName, Rating, CocoaPercent, C_beans.EnglishName
    FROM Bars B JOIN Countries C_companies ON B.CompanyLocationId = C_companies.Id 
        JOIN Countries C_beans ON B.BroadBeanOriginId = C_beans.Id
    {filters}
    ORDER BY {key} {order}
    LIMIT {num_entries}
    """.format

    # process group 1 and 2 parameters
    group1, group2 = parsed_dict["groups"][:2]
    if group1 is None:
        filters = ""
    else:
        g1_key, g1_val = group1.split("=")
        if group2 == "sell":
            if g1_key == "country":
                filters = f"WHERE C_companies.Alpha2 = '{g1_val}'"
            elif g1_key == "region":
                filters = f"WHERE C_companies.Region = '{g1_val}'"
        elif group2 == "source":
            if g1_key == "country":
                filters = f"WHERE C_beans.Alpha2 = '{g1_val}'"
            elif g1_key == "region":
                filters = f"WHERE C_beans.Region = '{g1_val}'"

    # process group 3 and 4 parameters
    group3, group4 = parsed_dict["groups"][2:4]
    if group3 == "ratings":
        key = "Rating"
    elif group3 == "cocoa":
        key = "CocoaPercent"

    if group4 == "top":
        order = "DESC"
    elif group4 == "bottom":
        order = "ASC"

    # process group 5 parameters
    group5 = parsed_dict["groups"][-1]  # Note this is an int.
    num_entries = group5

    return query(filters=filters, key=key, order=order, num_entries=num_entries)


def query_companies(parsed_dict, cmd):
    """
    Using a dict representing parsed command from extract_and_group_commands(.),
    validate parameters and construct SQL for high-level command "countries".
    Note a user can add parameters in arbitrary order.

    Parameters
    ----------
    parsed_dict: dict
        A list of parsed symbols.
    cmd: str
        Original command used for error message.

    Returns
    -------
    query: str
        The appropriate SQL for the user input.
    """
    assert parsed_dict["high_level"] == "companies", "wrong function used"
    error_msg = f"Command not recognized (invalid selection of parameters): {cmd}"
    # user cannot input group 2 parameters
    if not parsed_dict["is_user_input"][1] == -1:
        raise InvalidInputError(error_msg)

    query = """
    SELECT Company, C_companies.EnglishName, {aggregate}
    FROM Bars B JOIN Countries C_companies ON B.CompanyLocationId = C_companies.Id 
    {filters}
    GROUP BY Company
    HAVING COUNT(SpecificBeanBarName) > 4
    ORDER BY {key} {order}
    LIMIT {num_entries}
    """.format

    # process group 1 and 2 parameters
    group1, group2 = parsed_dict["groups"][:2]
    if group1 is None:
        filters = ""
    else:
        g1_key, g1_val = group1.split("=")
        if g1_key == "country":
            filters = f"WHERE C_companies.Alpha2 = '{g1_val}'"
        elif g1_key == "region":
            filters = f"WHERE C_companies.Region = '{g1_val}'"

    # process group 3 and 4 parameters
    group3, group4 = parsed_dict["groups"][2:4]
    if group3 == "ratings":
        aggregate = "AVG(Rating) AS R_AVG"
        key = "R_AVG"
    elif group3 == "cocoa":
        aggregate = "AVG(CocoaPercent) AS CP_AVG"
        key = "CP_AVG"
    elif group3 == "number_of_bars":
        # aggregate = "COUNT(DISTINCT SpecificBeanBarName) AS B_CNT"
        aggregate = "COUNT(SpecificBeanBarName) AS B_CNT"
        key = "B_CNT"

    if group4 == "top":
        order = "DESC"
    elif group4 == "bottom":
        order = "ASC"

    # process group 5 parameters
    group5 = parsed_dict["groups"][-1]  # Note this is an int.
    num_entries = group5

    return query(aggregate=aggregate, filters=filters, key=key, order=order, num_entries=num_entries)


def query_countries(parsed_dict, cmd):
    """
    Using a dict representing parsed command from extract_and_group_commands(.),
    validate parameters and construct SQL for high-level command "companies".
    Note a user can add parameters in arbitrary order.

    Parameters
    ----------
    parsed_dict: dict
        A list of parsed symbols.
    cmd: str
        Original command used for error message.

    Returns
    -------
    query: str
        The appropriate SQL for the user input.
    """
    assert parsed_dict["high_level"] == "countries", "wrong function used"
    error_msg = f"Command not recognized (invalid selection of parameters): {cmd}"

    query = """
    SELECT {countries}, {regions}, {aggregate}
    FROM Bars B JOIN Countries C_companies ON B.CompanyLocationId = C_companies.Id
        JOIN Countries C_beans ON B.BroadBeanOriginId = C_beans.Id
    {filters}
    GROUP BY {grouping}
    HAVING COUNT(SpecificBeanBarName) > 4
    ORDER BY {key} {order}
    LIMIT {num_entries}
    """.format

    # process group 1 and 2 parameters
    group1, group2 = parsed_dict["groups"][:2]
    if group2 == "sell":
        grouping = "C_companies.EnglishName"
        countries = "C_companies.EnglishName"
        regions = "C_companies.Region"
    elif group2 == "source":
        grouping = "C_beans.EnglishName"
        countries = "C_beans.EnglishName"
        regions = "C_beans.Region"

    if group1 is None:
        filters = ""
    else:
        g1_key, g1_val = group1.split("=")
        if group2 == "sell":
            if g1_key == "country":
                raise InvalidInputError(error_msg)
            elif g1_key == "region":
                filters = f"WHERE C_companies.Region = '{g1_val}'"
        elif group2 == "source":
            if g1_key == "country":
                raise InvalidInputError(error_msg)
            elif g1_key == "region":
                filters = f"WHERE C_beans.Region = '{g1_val}'"

    # process group 3 and 4 parameters
    group3, group4 = parsed_dict["groups"][2:4]
    if group3 == "ratings":
        aggregate = "AVG(Rating) AS R_AVG"
        key = "R_AVG"
    elif group3 == "cocoa":
        aggregate = "AVG(CocoaPercent) AS CP_AVG"
        key = "CP_AVG"
    elif group3 == "number_of_bars":
        # aggregate = "COUNT(DISTINCT SpecificBeanBarName) AS B_CNT"
        aggregate = "COUNT(SpecificBeanBarName) AS B_CNT"
        key = "B_CNT"

    if group4 == "top":
        order = "DESC"
    elif group4 == "bottom":
        order = "ASC"

    # process group 5 parameters
    group5 = parsed_dict["groups"][-1]  # Note this is an int.
    num_entries = group5

    return query(aggregate=aggregate, filters=filters, key=key, order=order, num_entries=num_entries,
                 grouping=grouping, countries=countries, regions=regions)


def query_regions(parsed_dict, cmd):
    """
    Using a dict representing parsed command from extract_and_group_commands(.),
    validate parameters and construct SQL for high-level command "companies".
    Note a user can add parameters in arbitrary order.

    Parameters
    ----------
    parsed_dict: dict
        A list of parsed symbols.
    cmd: str
        Original command used for error message.

    Returns
    -------
    query: str
        The appropriate SQL for the user input.
        """
    assert parsed_dict["high_level"] == "regions", "wrong function used"
    error_msg = f"Command not recognized (invalid selection of parameters): {cmd}"
    # user can't input group 1 parameters
    if not parsed_dict["is_user_input"][0] == -1:
        raise InvalidInputError(error_msg)

    query = """
    SELECT {regions}, {aggregate}
    FROM Bars B JOIN Countries C_companies ON B.CompanyLocationId = C_companies.Id
        JOIN Countries C_beans ON B.BroadBeanOriginId = C_beans.Id
    GROUP BY {grouping}
    HAVING COUNT(SpecificBeanBarName) > 4
    ORDER BY {key} {order}
    LIMIT {num_entries}
    """.format

    # process group 1 and 2 parameters
    _, group2 = parsed_dict["groups"][:2]
    if group2 == "sell":
        grouping = "C_companies.Region"
        regions = "C_companies.Region"
    elif group2 == "source":
        grouping = "C_beans.Region"
        regions = "C_beans.Region"

    # process group 3 and 4 parameters
    group3, group4 = parsed_dict["groups"][2:4]
    if group3 == "ratings":
        aggregate = "AVG(Rating) AS R_AVG"
        key = "R_AVG"
    elif group3 == "cocoa":
        aggregate = "AVG(CocoaPercent) AS CP_AVG"
        key = "CP_AVG"
    elif group3 == "number_of_bars":
        # aggregate = "COUNT(DISTINCT SpecificBeanBarName) AS B_CNT"
        aggregate = "COUNT(SpecificBeanBarName) AS B_CNT"
        key = "B_CNT"

    if group4 == "top":
        order = "DESC"
    elif group4 == "bottom":
        order = "ASC"

    # process group 5 parameters
    group5 = parsed_dict["groups"][-1]  # Note this is an int.
    num_entries = group5

    return query(aggregate=aggregate, key=key, order=order, num_entries=num_entries, grouping=grouping, regions=regions)


def load_help_text():
    with open('Proj3Help.txt') as f:
        return f.read()


# Part 2 & 3: Implement interactive prompt and plotting. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        if response == "exit":
            break

        if len(response) == 0:
            continue

        if response == 'help':
            print(help_text)
            continue

        try:
            results = process_command(response)
            parsed_dict = extract_and_group_commands(response)
            high_level = parsed_dict["high_level"]
            if not parsed_dict["barplot"]:
                for record in results:
                    print_record(record, high_level)
                print()
            else:
                barplot(results, parsed_dict["groups"][2], parsed_dict["high_level"])
        except InvalidInputError as e:
            print(e)
            print()

    print("\nBye!")


def print_record(record, high_level, text_len=12):
    """
    Helper function for part 2. Formatted print of one record as a tuple based on type of the high-level command.

    Parameters
    ----------
    record: tuple
        One record from query results list.
    high_level: str
        The high-level command.
    text_len: int
        Maximum length of text fields.

    Returns
    -------
    None
    """
    text_width = text_len + 4
    numeric_width = 7
    if high_level == "bars":
        for ind, entry in enumerate(record):
            if isinstance(entry, str):
                if len(entry) > text_len:
                    entry = entry[:text_len] + "..."
                print(f"{entry:{text_width}}", end="")
            elif ind == 3:
                print(f"{entry:<{numeric_width}.1f}", end="")
            elif ind == 4:
                print(f"{entry:<{numeric_width}.0%}", end="")

    elif high_level in ["companies", "countries", "regions"]:
        for ind, entry in enumerate(record):
            if isinstance(entry, str):
                if len(entry) > text_len:
                    entry = entry[:text_len] + "..."
                print(f"{entry:{text_width}}", end="")
            elif isinstance(entry, int):
                print(f"{entry:<{numeric_width}d}", end="")
            elif isinstance(entry, float):
                print(f"{entry:<{numeric_width}.1f}", end="")

    print()


def barplot(records, g3_param, high_level):
    """
    Make a bar chart and show it if "barplot" is True.

    Parameters
    ----------
    records: list
        A list of tuples. Query results.
    g3_param: str
        Group 3 parameters: ratings, cocoa or number_of_bars.
    high_level: str
        The high-level command.

    Returns
    -------
    None
    """
    def const_fact(val):
        return lambda: val

    xvals = [record[0] for record in records]
    keys = {"bars": {"rating": 3, "cocoa": 4},
            "companies": defaultdict(const_fact(-1)),
            "countries": defaultdict(const_fact(-1)),
            "regions": defaultdict(const_fact(-1))}
    yvals = [record[keys[high_level][g3_param]] for record in records]
    trace = go.Bar(x=xvals, y=yvals)
    data = [trace]
    if high_level == "bars":
        layout = {"xaxis": {"tickangle": 20}}
    else:
        layout = {}
    fig = go.Figure(data=data, layout=layout)
    fig.show()


# Make sure nothing runs or prints out when this file is run as a module/library
if __name__ == "__main__":
    interactive_prompt()
