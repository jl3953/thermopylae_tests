# How to add your config
1) Make a copy of `src/config_object.py` and name it `trial_<whatever_you_want>.py`.
The `.gitignore` will ignore it in the directory. 
2) Change the fields that you need to. Add ones you need.
    - You may need to implement new functionality that goes along with any new
    fields.
3) Determine what latency throughput files should match it (choose the range and
step_size). See `config/lt.ini` for the default example.
4) In `src/main.py`, add your new `trial_<whatever_you_want>.py` file with the
filepath of the latency throughput file to the configuration section. Remember to
import the config object files in `src/main.py`.
5) Make sure the sqlite database directory is what you want it to be (by default, 
it is set to to `/proj/cops-PG0/workspaces/jl87`)
6) From the git root, run: `./src/main.py`

### Need to Implement
- Automatic start-up of chosen hotshard node.
- Extracting of the cockroach commit in the copied parameter ini files instead of
just the branch name, which may or may not exist at a further point in time.

### Not Implemented
- Partition affinity
