from datetime import datetime
import matplotlib.pyplot as plt

def parse_timestamp(ts_str):
    return datetime.strptime(ts_str, "%H:%M:%S.%f")

def partition_and_compute_differences():
    with open('bench/timestamps.txt', 'r') as f_timestamps, open('bench/io_timestamps.txt', 'r') as f_io_timestamps:
        current_timestamp = f_timestamps.readline().strip()
        prev_timestamp = current_timestamp
        
        subsequence_lengths = []
        
        time_differences = []
        
        io_line = f_io_timestamps.readline().strip()
        count = 0
        
        while current_timestamp:
            while io_line and io_line < current_timestamp:
                count += 1
                io_line = f_io_timestamps.readline().strip()
            
            subsequence_lengths.append(count)
            count = 0
            
            if prev_timestamp:
                current_time = parse_timestamp(current_timestamp)
                prev_time = parse_timestamp(prev_timestamp)
                time_diff_ms = (current_time - prev_time).total_seconds() * 1000  # Convert to milliseconds
                time_differences.append(time_diff_ms)
            
            prev_timestamp = current_timestamp
            current_timestamp = f_timestamps.readline().strip()

        while io_line:
            count += 1
            io_line = f_io_timestamps.readline().strip()
        subsequence_lengths.append(count)
            
    return subsequence_lengths, time_differences

def plot_subsequence_lengths_and_time_diffs(subseq_lengths, time_diffs):
    gets_subseq = subseq_lengths[1::2]
    puts_subseq = subseq_lengths[0::2] 
    gets_time_diff = time_diffs[1::2] 
    puts_time_diff = time_diffs[0::2] 
    
    _, ax1 = plt.subplots(figsize=(10, 6))
    ax1.plot(gets_subseq, label='GETS', color='#66b3ff', linestyle='-')
    ax1.plot(puts_subseq, label='PUTS', color='#ff6666', linestyle='-')
    ax1.set_xlabel('Batch Index')
    ax1.set_ylabel('I/O requests')
    ax1.set_title('I/O Requests per 10,000 Commands')
    ax1.legend(loc='upper left')

    _, ax2 = plt.subplots(figsize=(10, 6))
    ax2.plot(gets_time_diff, label='GETS', color='#66b3ff', linestyle='-')
    ax2.plot(puts_time_diff, label='PUTS', color='#ff6666', linestyle='-')
    ax2.set_xlabel('Batch Index')
    ax2.set_ylabel('Latency (ms)')
    ax2.set_title('Latency per 10,000 Commands')
    ax2.legend(loc='upper left')

    plt.tight_layout()
    plt.show()

subseq_lengths, time_diffs = partition_and_compute_differences()
time_diffs[0] = time_diffs[2] # unfortunately did not store initial time of connection, so just copy the next

print(subseq_lengths, time_diffs)
plot_subsequence_lengths_and_time_diffs(subseq_lengths, time_diffs)