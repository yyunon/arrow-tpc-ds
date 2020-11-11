.PHONY: clean sim gui

all:
	python3 generate-input.py
	fletchgen -n SFilter -r recordbatch.rb -s memory.srec -l vhdl --sim --axi
generate-old:
	python3 generate-input_v_0.py
interface-old:
	fletchgen -n SFilter -r recordbatch.rb -s memory.srec -l vhdl --sim --axi
generate-new:
	python3 generate-input.py
interface-new:
	fletchgen -n SFilter -r recordbatch.rb -s memory.srec -l vhdl --sim --axi
clean:
	# input files
	rm -f recordbatch.rb
	# vhdmmio stuff	
	rm -rf vhdmmio-doc
	rm -f fletchgen.mmio.yaml
	rm -f vhdmmio.log
	# fletchgen stuff
	rm -f memory.srec
	rm -rf dot
	rm -f vhdl/*.gen.vhd
