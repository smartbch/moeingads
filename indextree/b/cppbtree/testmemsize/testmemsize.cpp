#include <iostream>
#include <fstream>
#include "../cppbtree.cpp"

using namespace std;

union u64_or_b8 {
	char buffer[8];
	uint64_t u64;
};

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
	auto zero = bits48::from_uint64(0);
	while(1) {
		if(total%500000==0) cout<<"Now "<<total<<endl;
		u64_or_b8 data;
		if(!myFile.read(data.buffer, 8)) {
			break;
		}
		bt.set(data.u64>>48, bits48::from_uint64(data.u64), zero);
		total++;
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
