cd C:\\works\\rust\\prism-rust\\target\\debug

mkdir -p test1
cd test1

../prism.exe --p2p 127.0.0.1:7001 --p2p-id 0 --voter-chains 2 --known-peer 127.0.0.1:7000

cd ..

mkdir -p test2
cd test2
../prism.exe --p2p 127.0.0.1:7000 --p2p-id 1 --voter-chains 2 --known-peer 127.0.0.1:7001
