#include <string>
#include <fstream>

#include <grpcpp/grpcpp.h>
#include <chrono>
#include <thread>
#include <cstdio>
#include "masterslave.grpc.pb.h"
#include <iostream>
#include <cctype>
#include <unistd.h>
#include <unordered_map>
#include <hdfs.h>
using namespace std;

// User grpc Functions
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

// Use prod file structures
using masterslave::Data;
using masterslave::Ping;
using masterslave::Reply;
using masterslave::Signal;
using masterslave::Slave;

// class for service implementation
class MasterSlaveService final : public Ping::Service
{

public:
    string filename;
    vector<string> values;

private:
    Status sendRequest(
        ServerContext *context,
        const Signal *s,
        Reply *reply) override
    {
        // override the function for using our own implementation
        // check the type of signal and send the appropriate response
        if (s->signal() == "Heartbeat")
            reply->set_result("Responsive");

        return Status::OK;
    }


    //return word and frequency
    string splitstring(string input, char delimeter, int &val)
    {
        string word;
        string value;

        int i = 0;
        //keep iterating until a delimeter is found
        for (; input[i] != delimeter; i++)
        {
            word.push_back(input[i]);
        }

        i++;
        for (; i < input.size(); i++)
        {
            value.push_back(input[i]);
        }

        val = stoi(value);

        return word;
    }

    // map task for each line
    void maptask(string data, string filename)
    {
        // open the existing file and append key value pairs
        ofstream fout;
        fout.open(filename, ios::app);

        string word = "";
        data.push_back('\n');

        // traverse the line and write word count of each word
        for (int i = 0; i < data.size(); i++)
        {
            if (isalpha(data[i]))
                word.push_back(data[i]);
            else
            {
                word += ",1";
                fout << word << "\n";
                word.clear();
            }
        }
    }

    // reduce helper task
    void reducetask(char key, int filecount)
    {

        // file name start form 5000.txt,5001.txt......
        int filenum = 5000;

        // hash map to find frequency in local file
        unordered_map<std::string, int> word_count;
        string word;

        // traverse all files for a single key
        for (int i = 0; i < filecount; i++)
        {
            string file = to_string(filenum);
            file += ".txt";
            ifstream fin(file);

            fin >> word;

            // traverse the file and calculate all frequencies in local file saved
            while (fin >> word)
            {
                if (word[0] == key)
                {
                    int val;
                    string splitword = splitstring(word, ',', val);

                    // store the value and word frequency
                    word_count[splitword] += val;
                }
                else if (word[0] < key)
                    continue;
                else
                    break;
            }

            fin.close();

            filenum += 1;
        }

        // write the reduced output
        string output = "output" + filename;
        ofstream fout(output, std::ios_base::app);

        for (const auto &[word, count] : word_count)
        {
            fout << word << " " << count << endl;
        }

        fout.close();
    }

    // sort the calculated word frequencies
    void sorttask()
    {

        string word;
        int val1, val2;
        values.clear();

        ifstream fin(filename);
        ofstream fout;

        // store values in a vector
        while (fin >> word)
        {
            values.push_back(word);
        }
        fin.close();

        // sort the values
        sort(values.begin(), values.end());

        // calculate the values of sorted words
        fout.open(filename);
        int frequency = 0;
        for (int i = 0; i + 1 < values.size(); i++)
        {

            // split the word and value
            string curr_word = splitstring(values[i], ',', val1);
            string next_word = splitstring(values[i + 1], ',', val2);

            // check for continous words
            if (curr_word == next_word)
            {
                frequency += val1;
            }
            else
            {

                // write the frequencies
                frequency += val1;
                curr_word += ',';
                curr_word += to_string(frequency);
                fout << curr_word << "\n";
                frequency = 0;
            }
        }
        values.clear();
        fout.close();
    }


    //read the file and store it in a vector
    vector<string> readfile(string input_path)
    {

        std::vector<std::string> lines;
        hdfsFS fs = hdfsConnect("default", 0);

        // Open file for reading
        hdfsFile file = hdfsOpenFile(fs, input_path.c_str(), O_RDONLY, 0, 0, 0);
        if (!file)
        {
            std::cerr << "Failed to open file" << std::endl;
            return lines;
        }

        // Read file into buffer
        const int buffer_size = 1024;
        char buffer[buffer_size];
 
        while (true)
        {
            tSize num_bytes_read = hdfsRead(fs, file, buffer, buffer_size);
            if (num_bytes_read == -1)
            {
                std::cerr << "Failed to read file" << std::endl;
                return lines;
            }
            if (num_bytes_read == 0)
            {
                // End of file
                break;
            }
            // Add buffer to lines
            lines.emplace_back(buffer, num_bytes_read);
        }

        // Close file and disconnect from HDFS
        hdfsCloseFile(fs, file);
        hdfsDisconnect(fs);

        // // Print lines
        // for (const auto &line : lines)
        // {
        //     std::cout << line << std::endl;
        // }

        return lines;
    }

    Status map(
        ServerContext *context,
        const Data *requests,
        Reply *reply

        ) override
    {
        vector<thread> threads;
        cout.flush();
              cout << "Data received:" << endl
             << endl;


        // extract the grpc data
        for (int i = 0; i < requests->data_size(); i++)
        {
            vector<string> lines = readfile(requests->data(i));
            for (int j = 0; j < lines.size(); j++)
            {
                threads.push_back(thread(&MasterSlaveService::maptask, this, lines[j], filename));
            }
        }

  
        cout << "Performing map tasks" << endl
             << endl;

        // wait for all threads to complete
        for (int i = 0; i < threads.size(); i++)
        {
            threads[i].join();
        }

        sorttask();
        reply->set_result("Task completed");
        return Status::OK;
    }

    Status reduce(
        ServerContext *context,
        const Data *requests,
        Reply *reply

        ) override
    {
        vector<thread> threads;
        cout.flush();

        cout << "Performing reduce task" << endl;

        // extract the grpc data
        char si = char(stoi(requests->data(0)));
        char ei = char(stoi(requests->data(1)));
        int filecount = stoi(requests->data(2));

        // calculate the output for required key parition in seperate thread
        for (char i = si; i <= ei; i++)
        {
            threads.push_back(thread(&MasterSlaveService::reducetask, this, i, filecount));
        }

        // wait for threads to complete
        for (int i = 0; i < threads.size(); i++)
        {
            threads[i].join();
        }
        reply->set_result("Task completed");
        return Status::OK;
    }
};

// function for listening requests
void listenForRequest(Slave &s, string address)
{
    // create a service object for using the service of prod file
    MasterSlaveService service;
    service.filename = address + ".txt";

    // builder object for establishing a connection between master and slave
    ServerBuilder builder;

    builder.AddListeningPort(s.address(), grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    // start the service and await for incoming requests
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on port: " << s.address() << std::endl;

    server->Wait();
}

// testing function
void Run(int argc, char **argv)
{

    // create a slave object and assign its ipaddress and port
    Slave s;
    string address;

    if (argc >= 2)
    {
        address = "0.0.0.0:";
        address += argv[1];
        s.set_address(address);
        address = argv[1];
    }
    else
    {
        address = "5000";
        s.set_address("0.0.0.0:5000");
    }
    listenForRequest(s, address);
}



// main function
int main(int argc, char **argv)
{
    setenv("CLASSPATH", "/home/faizan/hadoop-3.3.5/etc/hadoop:/home/faizan/hadoop-3.3.5/share/hadoop/common/*:/home/faizan/hadoop-3.3.5/share/hadoop/common/lib/*:/home/faizan/hadoop-3.3.5/share/hadoop/hdfs/*:/home/faizan/hadoop-3.3.5/share/hadoop/hdfs/lib/*:/home/faizan/hadoop-3.3.5/share/hadoop/mapreduce/*:/home/faizan/hadoop-3.3.5/share/hadoop/mapreduce/lib/*", 1);
    Run(argc, argv);
    return 0;
}
