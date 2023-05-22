#include <unistd.h>

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <cstdio>
#include <stdio.h>
#include <sys/select.h>
#include <termios.h>
#include <sys/ioctl.h>
#include <string>
#include <queue>
#include <hdfs.h>

#include "masterslave.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#define fail "Not responsive"
#define complete "Task completed"
#define progress "In progress"
using namespace std;

// import grpc libraries
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

// import the prodfile structures
using masterslave::Data;
using masterslave::Master;
using masterslave::Ping;
using masterslave::Reply;
using masterslave::Signal;
using masterslave::Slave;

struct WordCount
{
  string word;
  int count;

  bool operator<(const WordCount &other) const
  {
    return count > other.count;
  }
};

class SlaveServer
{
private:
  // declase a stub for using the service of prod file
  std::unique_ptr<Ping::Stub> stub_;

public:
  SlaveServer(std::shared_ptr<Channel> channel)
      : stub_(Ping::NewStub(channel)) {}

  // function for sending request
  string sendRequest(Master &m, string signal)
  {
    Signal s;
    Reply reply;
    ClientContext context;

    // assign the value of the signal from input
    s.set_signal(signal);

    // Set the timeout interval , return status not ok if time passes deadline
    context.set_deadline(chrono::system_clock::now() + chrono::seconds(m.timeout_interval()));
    // send the request to the slave and await for the response
    Status status = stub_->sendRequest(&context, s, &reply);

    // check for the status of the response message
    if (status.ok())
    {
      return reply.result();
    }
    else
    {
      // std::cout << status.error_code() << ": " << status.error_message()<<
      // std::endl;
      return fail;
    }
  }

  string sendMapTask(int si, int ei, vector<string> filename)
  {
    Reply reply;
    Data request;
    ClientContext context;

    for (int i = si; i < ei; i++)
    {
      request.add_data(filename[i]);
    }
    Status status = stub_->map(&context, request, &reply);
    if (status.ok())
    {
      return reply.result();
    }
    else
    {
      return fail;
    }
  }

  string sendReduceTask(int si, int ei, int filecount)
  {
    Reply reply;
    Data request;
    ClientContext context;

    request.add_data(to_string(si));
    request.add_data(to_string(ei));
    request.add_data(to_string(filecount));

    Status status = stub_->reduce(&context, request, &reply);
    if (status.ok())
    {
      return reply.result();
    }
    else
    {
      return fail;
    }
  }
};

// function to implement keyboard hit
int _kbhit()
{
  static const int STDIN = 0;
  static bool initialized = false;

  if (!initialized)
  {
    // Use termios to turn off line buffering
    termios term;
    tcgetattr(STDIN, &term);
    term.c_lflag &= ~ICANON;
    tcsetattr(STDIN, TCSANOW, &term);
    setbuf(stdin, NULL);
    initialized = true;
  }

  int bytesWaiting;
  ioctl(STDIN, FIONREAD, &bytesWaiting);
  return bytesWaiting;
}

// set the status of each task
void setTaskStatus(Master &m, int num_of_task)
{

  m.clear_taskstatus();
  m.clear_taskid();
  for (int i = 0; i < num_of_task; i++)
  {
    m.add_taskstatus("Not assigned");
    m.add_taskid("NA");
  }
}

// set initial parameters for master
void setParameters(Master &master, int num_of_task)
{
  vector<string> address{"0.0.0.0:5000", "0.0.0.0:5001", "0.0.0.0:5002"};
  master.set_slavecount(address.size());

  // init the addresses
  for (int i = 0; i < address.size(); i++)
  {
    master.add_addresses(address[i]);
    master.add_status("NA");
  }

  // assign the intervals
  int ci;
  cout << "Enter the control interval: ";
  cin >> ci;

  master.set_control_interval(ci);
  master.set_timeout_interval(ci * 4);
}



//check status of each salve
bool checkOnlineSlaves(Master &m)
{

  int count = 0;
  for (int i = 0; i < m.slavecount(); i++)
  {
    if (m.status(i) == fail)
      count++;
  }

  return count == m.slavecount();
}

// return index of a free slave
int checkFreeSlave(Master &m, int task_division)
{

  int si = 0;
  for (int i = 0; i < m.slavecount(); i++)
  {
    bool free = true;

    // check for each task on a single machine
    for (int j = si; j < si + task_division; j++)
    {

      // if task is not complete then break
      if (m.taskstatus(j) != complete)
      {
        free = false;
        break;
      }
    }

    si += task_division;

    // as soon as it finds a free slave,return it
    if (free)
      return i;
  }

  return -1;
}

// check all the completed tasks
bool checkCompleteTasks(Master &m, int num_of_task)
{
  for (int i = 0; i < num_of_task; i++)
  {

    // check the progress of each task
    if (m.taskstatus(i) == progress)
    {
      return false;
    }
  }

  return true;
}




// print completed tasks
void printCompleteTasks(Master &m, int num_of_tasks)
{
  int count = 0;
  for (int i = 0; i < num_of_tasks; i++)
  {
    if (m.taskstatus(i) == complete)
      count++;
  }

  cout << "Tasks completed: " << count << "/" << num_of_tasks << endl;
}

// get the status of each slave
void getStatus(Master &m)
{

  cout << "--------------------------------------------------------------------------------------" << endl
       << endl;
  cout << "Status of each slave:" << endl;
  for (int i = 0; i < m.slavecount(); i++)
  {
    cout << "Server " << m.addresses(i) << " is " << m.status(i) << endl;
  }
  cout << "--------------------------------------------------------------------------------------" << endl
       << endl;
}

// get the status of each task
void getTaskStatus(Master &m, int no_of_tasks)
{

  cout << "--------------------------------------------------------------------------------------" << endl
       << endl;
  cout << "Total tasks: " << no_of_tasks << endl;
  for (int i = 0; i < no_of_tasks; i++)
  {
    cout << "Task " << i + 1 << " is assigned to " << m.taskid(i) << endl;
  }

  cout << endl;
  for (int i = 0; i < no_of_tasks; i++)
  {
    cout << "Task " << i + 1 << " status: " << m.taskstatus(i) << endl;
  }

  printCompleteTasks(m, no_of_tasks);
  cout << "--------------------------------------------------------------------------------------" << endl
       << endl;
}

// dynamically update the control interval
void updateControlInterval(Master &m)
{
  int input;
  cout << "Enter new interval:";
  cin >> input;
  m.set_control_interval(input);
}




// function for pinging to slaves
void sendSignalToSlave(Master &m, string address, int i)
{
  // establish a connection to a slave using its address
  SlaveServer client(
      grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));

  // send the signal and await for the response
  string response = client.sendRequest(m, "Heartbeat");
  m.set_status(i, response);
}

// utility function for pining
void sendSignals(Master &m)
{
  string status;
  vector<thread> threads;

  // send signal in a seperate thread to each slave using its ip address and socket
  for (int i = 0; i < m.slavecount(); i++)
  {
    threads.push_back(thread(sendSignalToSlave, ref(m), m.addresses(i), i));
  }

  // wait for all threads to complete execution
  for (int i = 0; i < threads.size(); i++)
  {
    threads[i].join();
  }
}




// function for sending tasks to all slaves
void sendMapTasktoSlave(Master &m, string address, int si, int ei, vector<string> filename)
{
  // establish a connection to a slave using its address
  SlaveServer client(
      grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));

  // send the signal and await for the response
  string response = client.sendMapTask(si, ei, filename);

  for (int i = si; i < ei; i++)
    m.set_taskstatus(i, response);
}

// utility function for assigning tasks
vector<thread> sendMapTasks(Master &m, int num_of_task, vector<string> filename)
{
  vector<thread> threads;
  setTaskStatus(m, num_of_task);

  // divide the workload among slaves
  int task_division = num_of_task / m.slavecount();
  int task_remainder = num_of_task % m.slavecount();
  int si = 0;
  int ei = 0;

  cout << "---> Starting map task" << endl;
  // send signal in a seperate thread to each slave using its ip address and socket
  for (int i = 0; i < m.slavecount(); i++)
  {

    if (i != m.slavecount() - 1)
      ei = si + task_division;
    else
      ei = si + task_division + task_remainder;

    for (int j = si; j < ei; j++)
    {

      m.set_taskid(j, m.addresses(i));
      m.set_taskstatus(j, progress);
    }

    threads.push_back(thread(sendMapTasktoSlave, ref(m), m.addresses(i), si, ei, filename));
    si += task_division;
  }

  return threads;
}

// check for failed map tasks
void assignFailedMapTask(Master &m, int num_of_task, vector<string> filename)
{

  vector<thread> threads;
  int task_division = num_of_task / m.slavecount();

  // check for tasks that did not complete
  cout << "--->Handling failed tasks" << endl;
  int si = 0, ei = 0;
  int count = 0;

  bool first = true;
  for (int i = 0; i < num_of_task; i++)
  {

    // check for failed task index
    if (m.taskstatus(i) == fail)
    {

      // calculate the number of tasks failed and assign them in batch
      if (first)
      {
        si = i;
        ei = si + 1;
        first = false;
      }
      else if (first == false)
      {
        ei++;
      }

      // after calculating batch, assign them to a fee slave
      if (i == num_of_task - 1 || m.taskstatus(i + 1) != fail)
      {
        int freeslave = checkFreeSlave(m, task_division);

        m.set_taskid(i, m.addresses(freeslave));
        m.set_taskstatus(i, progress);
        threads.push_back(thread(sendMapTasktoSlave, ref(m), m.addresses(freeslave), si, ei, filename));

        first = true;
        si = 0;
        ei = 0;
      }
    }
  }

  // join the threads
  for (int i = 0; i < threads.size(); i++)
  {
    threads[i].join();
  }
}




// send task to a single slave
void sendReduceTaskToSlave(Master &m, string address, int i, int si, int ei)
{
  // establish a connection to a slave using its address
  SlaveServer client(
      grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));

  // send the signal and await for the response
  string response = client.sendReduceTask(si, ei, m.slavecount());
  m.set_taskstatus(i, response);
}

// reduce function
vector<thread> sendReduceTask(Master &m)
{

  vector<thread> threads;
  setTaskStatus(m, m.slavecount());

  // parition based on alphabets
  int letters = 26;
  int partition = letters / m.slavecount();
  int partition_rem = letters % m.slavecount();

  char si = 'a';
  char ei;

  cout << "--->Starting reduce task" << endl;
  for (int i = 0; i < m.slavecount(); i++)
  {

    // check for boundry cases >z
    if (si + partition > 'z' + 1)
    {
      ei = 'A';
      int rem = 26 - partition;
      ei = ei + rem;
    }
    else
    {
      // add the reminder values in last iteration
      if (i != m.slavecount() - 1)
        ei = si + partition;
      else
        ei = si + partition + partition_rem;
      ei - 1;
    }

    m.set_taskid(i, m.addresses(i));
    m.set_taskstatus(i, progress);

    threads.push_back(thread(sendReduceTaskToSlave, ref(m), m.addresses(i), i, si, ei));
    si = ei + 1;
  }

  return threads;
}

// check for failed reduce tasks
void assignFailedReduceTask(Master &m)
{
  vector<thread> threads;

  // partition based on alphabets
  int letters = 26;
  int partition = letters / m.slavecount();
  int partition_rem = letters % m.slavecount();

  // check for tasks that did not complete
  cout << "--->Handling failed reduce tasks" << endl;
  char si = 'a';
  char ei;
  for (int i = 0; i < m.slavecount(); i++)
  {

    // check for boundry cases
    if (si + partition > 'z' + 1)
    {
      ei = 'A';
      int rem = 26 - partition;
      ei = ei + rem;
    }
    else
    {

      // on last iteration, add the remainder values
      if (i != m.slavecount() - 1)
        ei = si + partition;
      else
        ei = si + partition + partition_rem;
      ei - 1;
    }

    // find a free slave and assign this task
    if (m.taskstatus(i) == fail)
    {

      int freeslave = checkFreeSlave(m, 1);

      m.set_taskid(i, m.addresses(freeslave));
      m.set_taskstatus(i, progress);
      threads.push_back(thread(sendReduceTaskToSlave, ref(m), m.addresses(freeslave), i, si, ei));
    }
    si = ei + 1;
  }

  // join the threads
  for (int i = 0; i < threads.size(); i++)
  {
    threads[i].join();
  }
}




// ping to all slaves
void Ping(Master &m)
{
  sendSignals(m);

  // prediodically ping and wait for control interval
  std::this_thread::sleep_for(std::chrono::seconds(m.control_interval()));
}

// calculate top N max words
void calculateNword(Master &m)
{

  // Connect to HDFS
  string outputhdfs = "/faizan/output.txt";
  hdfsFS fs = hdfsConnect("default", 0);
  hdfsFile output_file = hdfsOpenFile(fs, outputhdfs.c_str(), O_WRONLY | O_CREAT, 0, 0, 0);

  // Process each chunk file
  int n;
  cout << "Enter value on N:";
  cin >> n;

  int num_chunks = m.slavecount();
  int chunkcount = 5000;

  // pq for storing word frequencies ina heap
  priority_queue<WordCount> word_counts;

  for (int i = 0; i < num_chunks; i++)
  {
    string filename = "output" + to_string(chunkcount) + ".txt";

    // if file does not open skip it
    ifstream fin(filename);
    if (!fin.is_open())
    {
      chunkcount++;
      continue;
    }

    // Read each word count from the chunk and update the min-heap
    string word;
    int count;
    while (fin >> word >> count)
    {

      // Convert the word count to a string
      string word_count = word + " " + to_string(count) + "\n";

      // Write the word count to the HDFS file
      hdfsWrite(fs, output_file, word_count.c_str(), word_count.size());

      if (!word.empty())
      {
        if (word_counts.size() < n || count > word_counts.top().count)
        {
          word_counts.push({word, count});
        }
        if (word_counts.size() > n)
        {
          word_counts.pop();
        }
      }
    }

    // close and delete file as they are read
    fin.close();
    remove(filename.c_str());
    string mapfile = to_string(chunkcount) + ".txt";
    remove(mapfile.c_str());

    chunkcount++;
  }

  // Close the input file and the output file in HDFS
  hdfsCloseFile(fs, output_file);

  // Disconnect from the HDFS client
  hdfsDisconnect(fs);

  // Output the top n word counts seen overall
  vector<WordCount> top_n_words;
  while (!word_counts.empty())
  {
    top_n_words.push_back(word_counts.top());
    word_counts.pop();
  }
  reverse(top_n_words.begin(), top_n_words.end());
  cout << endl;
  for (const auto &wc : top_n_words)
  {
    cout << wc.word << ": " << wc.count << endl;
  }
}

// take input on runtime
void KeyInputs(Master &m, int num_of_task, vector<string> filename)
{

  vector<thread> mapthreads;
  vector<thread> reducethreads;
  bool map = true;
  bool reduce = false;

  //if no slave is online ,terminate
  if (checkOnlineSlaves(m))
  {
    cout << "No slave is online" << endl
         << "Terminating Operation" << endl;
    return;
  }

  setTaskStatus(m, num_of_task);
  mapthreads = sendMapTasks(m, num_of_task, filename);
  
  getTaskStatus(m,num_of_task);

  while (true)
  {

    // detect which key has been pressed
    if (_kbhit())
    {
      if (getchar() == 't')
      {
        getTaskStatus(m, num_of_task);
      }

      else if (getchar() == 's')
      {
        sendSignals(m);
        getStatus(m);
      }
      else if (getchar() == 'c')
      {
        updateControlInterval(m);
      }
    }

    if (checkCompleteTasks(m, num_of_task) && map)
    {
      // wait for all mapthreads to complete execution
      for (int i = 0; i < mapthreads.size(); i++)
      {
        mapthreads[i].join();
      }

      assignFailedMapTask(m, num_of_task, filename);
      getTaskStatus(m,num_of_task);
      cout << endl
           << "Map 100% reduce 0%" << endl
           << endl;
      map = false;
      reduce = true;

      //start reduce after map has been completed
      reducethreads = sendReduceTask(m);
      num_of_task = m.slavecount();
      getTaskStatus(m,num_of_task);
    }
    else if (checkCompleteTasks(m, num_of_task) && reduce)
    {
      //join reduce threads as safety measure
      for (int i = 0; i < reducethreads.size(); i++)
      {
        reducethreads[i].join();
      }

      assignFailedReduceTask(m);
      getTaskStatus(m,num_of_task);
      
      cout << endl
           << "Map 100% reduce 100%" << endl
           << endl;
      calculateNword(m);
      reduce = false;
      return;
    }
  }
}



//load file and create chunks
int createchunks(vector<string> &filenames)
{
  // Connect to HDFS
  hdfsFS fs = hdfsConnect("default", 0);

  // Open input file
  const char *input_path = "/faizan/input.txt";
  hdfsFile input_file = hdfsOpenFile(fs, input_path, O_RDONLY, 0, 0, 0);
  if (!input_file)
  {
    std::cerr << "Failed to open input file " << input_path << std::endl;
    return 1;
  }

  // Get file size
  tOffset file_size = hdfsGetPathInfo(fs, input_path)->mSize;

  // Set chunk size and buffer size
  tOffset chunk_size = 1024 * 100; // 10 KB
  int buffer_size = 1024;

  // Allocate buffer
  char *buffer = new char[buffer_size];

  int count = 0;
  // Divide file into chunks
  for (tOffset offset = 0; offset < file_size; offset += chunk_size)
  {
    // Compute chunk size
    tOffset remaining_size = file_size - offset;
    tOffset current_chunk_size = min(chunk_size, remaining_size);

    // Create chunk file
    string chunk_path = "/faizan/hadoop/chunk" + to_string(count);
    filenames.push_back(chunk_path);
    hdfsFile chunk_file = hdfsOpenFile(fs, chunk_path.c_str(), O_WRONLY | O_CREAT, 0, 0, 0);
    if (!chunk_file)
    {
      cerr << "Failed to create chunk file " << chunk_path << endl;
      return 1;
    }

    // Write chunk data to file
    tOffset bytes_remaining = current_chunk_size;
    while (bytes_remaining > 0)
    {
      // Read data from input file
      int bytes_to_read = min((tOffset)buffer_size, bytes_remaining);
      int bytes_read = hdfsRead(fs, input_file, buffer, bytes_to_read);
      if (bytes_read < 0)
      {
        cerr << "Failed to read data from input file" << endl;
        return 1;
      }

      // Write data to chunk file
      int bytes_written = hdfsWrite(fs, chunk_file, buffer, bytes_read);
      if (bytes_written != bytes_read)
      {
        cerr << "Failed to write data to chunk file" << endl;
        return 1;
      }

      // Update bytes remaining
      bytes_remaining -= bytes_written;
    }

    // Close chunk file
    if (hdfsCloseFile(fs, chunk_file) != 0)
    {
      cerr << "Failed to close chunk file " << chunk_path << endl;
      return 1;
    }

    count++;
  }

  // Close input file
  if (hdfsCloseFile(fs, input_file) != 0)
  {
    cerr << "Failed to close input file " << input_path << endl;
    return 1;
  }

  // Free memory and disconnect from HDFS
  delete[] buffer;
  if (hdfsDisconnect(fs) != 0)
  {
    cerr << "Failed to disconnect from HDFS" << endl;
    return 1;
  }

  return 0;
}

// function for assigning tasks
void Run()
{

  vector<string> filenames;
  createchunks(filenames);
  Master master;
  vector<thread> threads;
  vector<string> input;

  string filename = "check.txt";
  int num_of_task = filenames.size();

  setParameters(master, num_of_task);
  cout << endl;

  // create seperate threads for main functionality and periodic pinging.
  threads.push_back(thread(Ping, ref(master)));
  this_thread::sleep_for(std::chrono::seconds(master.control_interval()));
  threads.push_back(thread(KeyInputs, ref(master), num_of_task, filenames));

  for (int i = 0; i < threads.size(); i++)
  {
    threads[i].join();
  }
}

// main function
int main(int argc, char *argv[])
{

  Run();
  setenv("CLASSPATH", "/home/faizan/hadoop-3.3.5/etc/hadoop:/home/faizan/hadoop-3.3.5/share/hadoop/common/*:/home/faizan/hadoop-3.3.5/share/hadoop/common/lib/*:/home/faizan/hadoop-3.3.5/share/hadoop/hdfs/*:/home/faizan/hadoop-3.3.5/share/hadoop/hdfs/lib/*:/home/faizan/hadoop-3.3.5/share/hadoop/mapreduce/*:/home/faizan/hadoop-3.3.5/share/hadoop/mapreduce/lib/*", 1);
  return 0;
}
