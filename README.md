# ep-meetings

The script :

1. first fetches the list of meetings on certain years.
	- The API call for this is https://data.europarl.europa.eu/api/v2/meetings?year={0}&format=application%2Fld%2Bjson&offset=0&limit=1000, with `{0}` replaced by a year
	- Example : https://data.europarl.europa.eu/api/v2/meetings?year=2025&format=application%2Fld%2Bjson&offset=0&limit=1000

2. then, for each meeting, retrieves the decisions taken during this meeting, and save this into a file
	- The API call for this is https://data.europarl.europa.eu/api/v2/meetings/{0}/decisions?vote-method=ROLL_CALL_EV&format=application%2Fld%2Bjson&json-layout=framed-and-included&offset=0&limit=1000, with `{0}` replaced by a meeting id, for example `MTG-PL-2025-01-20`
	- Example : https://data.europarl.europa.eu/api/v2/meetings/MTG-PL-2025-01-20/decisions?vote-method=ROLL_CALL_EV&format=application%2Fld%2Bjson&json-layout=framed-and-included&offset=0&limit=1000

3. then, conduct the analysis on a subset of the meetings (based on a parameter in the config file), and produces the CSV file