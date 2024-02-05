import matplotlib.pyplot as plt
import json

def plot_graph(file_name):
    # Read data from file
    file_path = file_name + '.tsv'
    with open(file_path, 'r') as file:
        data = file.read()

    # Split the TSV data into lines and extract relevant information
    lines = data.strip().split('\n')
    timestamps = []
    ops_per_second = []
    compaction_times = []
    compaction_time_period = []
    compaction_level_value = []

    manual_compaction_time_period = []
    manual_compaction_level_value = []

    # Write will always be the first line. 
    # If that is not true, something is wrong
    test_start_time = int(lines[0].split("\t")[1])
    initial_levels = [0] * 6
    manual_compaction_flag = False

    for line in lines:
        parts = line.split("\t")

        if parts[3] == 'write':
            start_time = int(parts[1]) - test_start_time
            end_time = int(parts[2]) - test_start_time
            ops_per_sec = float(parts[5])

            ops_per_second.append(((end_time)/1000000, ops_per_sec))
            # print(ops_per_second)

        elif parts[1] == 'Compaction':
            if parts[2] == 'Start':
                compaction_start_time = int(parts[3])
                if parts[4] == 'ManualCompaction':
                    manual_compaction_flag = True
            elif parts[2] == 'End':
                compaction_end_time = int(parts[3])
                total_compaction_time = int(parts[4])/1000000

                current_levels = list(map(int, parts[7].split(",")[1:-1]))
                
                level_diff = [current_levels[i] - initial_levels[i] if current_levels[i] - initial_levels[i] > 0 else 0 for i in range(len(current_levels))]
                initial_levels = current_levels

                total_files_compacted = sum(level_diff)
                assumed_level_start_time = (compaction_start_time - test_start_time)/1000000
                assumed_level_end_time = (compaction_start_time - test_start_time)/1000000
                for (level_num, level_files) in enumerate(level_diff):
                    if level_files != 0:
                        assumed_level_end_time = assumed_level_start_time + (total_compaction_time * (level_files / total_files_compacted))

                        # compaction_time_period.append(assumed_level_start_time)
                        # compaction_time_period.append(assumed_level_end_time)
                        # compaction_time_period.append(None)
                        # compaction_level_value.append(level_num)
                        # compaction_level_value.append(level_num)
                        # compaction_level_value.append(None)

                        # assumed_level_start_time = assumed_level_end_time

                        if manual_compaction_flag:
                            manual_compaction_time_period.append(assumed_level_start_time)
                            manual_compaction_time_period.append(assumed_level_end_time)
                            manual_compaction_time_period.append(None)
                            manual_compaction_level_value.append(level_num)
                            manual_compaction_level_value.append(level_num)
                            manual_compaction_level_value.append(None)

                            manual_compaction_flag = False

                        else:
                            compaction_time_period.append(assumed_level_start_time)
                            compaction_time_period.append(assumed_level_end_time)
                            compaction_time_period.append(None)
                            compaction_level_value.append(level_num)
                            compaction_level_value.append(level_num)
                            compaction_level_value.append(None)

                        assumed_level_start_time = assumed_level_end_time


    

    # Open the benchy.json file. Get the target from there and plot it
    benchy_file_path = '../benchy.json'

    target_ops_per_second = []
    with open(benchy_file_path, 'r') as benchy_file:
        benchy_data = json.load(benchy_file)

        for benchmark in benchy_data['benchmarks']:
            if benchmark['benchmark'] == 'fillrandom':
                start_time = int(benchmark['start_time'])
                end_time = int(benchmark['start_time']) + int(benchmark['duration'])

                target_ops_per_second.append([start_time, benchmark['throughput']])
                target_ops_per_second.append([end_time, benchmark['throughput']])

    # Plotting
    fig, ax1 = plt.subplots()

    # Plot operations per second
    ax1.set_xlabel('Time')
    ax1.set_ylabel('Operations per Second', color='tab:red')
    ops_timestamps, ops_per_second = zip(*ops_per_second)
    # print(ops_timestamps, ops_per_second)
    ax1.tick_params(axis='y', labelcolor='tab:red')
    ax1.set_ylim(bottom=0, top=350000)
    # ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d-%H:%M:%S'))
    ax1.plot(ops_timestamps, ops_per_second, color='tab:red')

    # Plot compaction time
    ax2 = ax1.twinx()
    ax2.set_ylabel('Compaction Time', color='tab:blue')
    ax2.plot(compaction_time_period, compaction_level_value, color='tab:blue', marker='+')
    ax2.set_ylim(bottom=-1, top=6)
    ax1.set_zorder(ax2.get_zorder() + 1)
    ax1.patch.set_visible(False)

    # Plot manual compaction time
    ax4 = ax1.twinx()
    # ax4.set_ylabel('Manual Compaction Time', color='tab:purple')
    ax4.plot(manual_compaction_time_period, manual_compaction_level_value, color='tab:purple', marker='x')
    ax4.set_ylim(bottom=-1, top=6)
    ax4.set_zorder(ax2.get_zorder() + 1)
    ax1.patch.set_visible(False)

    # create a third y-axis for ideal ops per second
    ax3 = ax1.twinx()
    ax3.set_ylim(bottom=0, top=350000)
    ax3.get_yaxis().set_visible(False)
    target_timestamps, target_values = zip(*target_ops_per_second)
    ax3.tick_params(axis='y', labelcolor='tab:green')
    ax3.set_ylim(bottom=0, top=350000)
    # Remove for sine wave implementation only
    # ax3.plot(target_timestamps, target_values, color='tab:green')

    plt.title('Operations and Compaction vs Time')

    # change dims
    fig.set_size_inches(18.5, 5.5)
    fig.savefig(file_name + '.png', dpi=300, bbox_inches='tight')

plot_graph('r4/default')
