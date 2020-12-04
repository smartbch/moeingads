#include <iostream>
#include <fstream>
#include "../cppbtree.cpp"

using namespace std;

int main(int argc, char** argv) {
	if(argc!=2) {
		cout<<"Usage: "<<argv[0]<<" <filename>"<<endl;
		return 1;
	}
	cout<<argv[1]<<endl;
	unsigned long long total = 0, max = 16*1024*1024;
	ifstream myFile(argv[1], ios::in | ios::binary);
	if (!myFile) {
		cout<<"Open failed!"<<endl;
		return 1;
	}
	BTree bt;
	while(1) {
		if(total%500000==0) cout<<"Now "<<total<<endl;
		char buffer[8];
		if(!myFile.read(buffer, 8)) {
			break;
		}
		if((buffer[0]&3)==0) {
			buffer[0]++;
		}
		mystr key(buffer, 8);
		if(bt.find(key) != bt.end()) {
			continue;
		}
		bt[key] = 0;
		total++;
		if(total != bt.size()) {
			cout<<"Err "<<total<<" "<<bt.size()<<endl;
			break;
		}
		if(total >= max) {
			break;
		}
	}
	cout<<"Size "<<bt.size()<<endl;
	cout<<"Press any key to exit"<<endl;
	char tmp;
	cin>>tmp;
	return 0;
}
