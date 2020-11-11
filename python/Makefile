.PHONY: clean sim gui

all:
	python3 generate-input.py
	fletchgen -n SFilter -r recordbatch.rb -s memory.srec -l vhdl --sim --axi
generate:
	python3 generate-input.py
interface:
	fletchgen -n SFilter -r recordbatch.rb -s memory.srec -l vhdl --sim --axi
clean:
	# input files
	rm -f *.rb
	# vhdmmio stuff	
	rm -rf vhdmmio-doc
	rm -f fletchgen.mmio.yaml
	rm -f vhdmmio.log
	# fletchgen stuff
	rm -f memory.srec
	rm -rf dot
	rm -f vhdl/*.gen.vhd
