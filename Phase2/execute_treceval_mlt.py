# Import all necessary modules
import subprocess
import os

# Get all the files from the directory mlt_tests
files = os.listdir('./mlt_tests/')

# Get all the files that start with the prefix 'qresults'
files = [e for e in files if e.startswith('qresults')]

# Initialize a list for the output
output_list = []

# For every file
for file in files:
    # Run trec_eval with the subprocess module using the '-m map' configuration to only get the map score
    p = subprocess.Popen(["trec_eval.exe", "-m", "map", "qrels.txt", "./mlt_tests/"+file], stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, shell=True)
    # Get the output pipeline and the error pipeline
    (output, err) = p.communicate()
    p_status = p.wait()
    # Decode the output using utf-8 as a list
    o = output.decode('utf-8').split()
    # Append filename to the output
    o.append(file)
    # Append the output to the output list
    output_list.append(o)

# Sort the list based on the MAP score
output_list = sorted(output_list, key=lambda x: x[2])

# Find the result with the default MLT parameters
default_pars = [e for e in output_list if e[3] == 'qresults_20_mlt_maqt25_mitf2_midf5_madf0_mism30.txt']
# Get the result with the worst MLT parameters
worst_pars = output_list[0]
# Get the result with the median of the MLT parameters
med_pars = output_list[len(output_list)//2]
# Get the result with the best MLT parameters
best_pars = output_list[-1]

# Print each result's output (that also has the filename)
print('Default MAP:', default_pars)
print('Worst MAP:', worst_pars)
print('Median MAP:', med_pars)
print('Best MAP:', best_pars)
