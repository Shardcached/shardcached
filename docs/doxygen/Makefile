docs: clean
	@ echo "Constructing a fake include directory hierarchy for Doxygen"
	@ mkdir headers
	@ mkdir headers/{libchash,libhl,libiomux,libshardcache}

	@ cp ../../deps/libshardcache/src/*h               headers/libshardcache/
	@ cp ../../deps/libshardcache/deps/libchash/*h     headers/libchash/
	@ cp ../../deps/libshardcache/deps/libiomux/src/*h headers/libiomux/
	@ cp ../../deps/libshardcache/deps/libhl/src/*h    headers/libhl/

	@ echo "Running doxygen"
	@ doxygen Doxyfile
	@ rm -fr headers

clean:
	@ echo "Cleaning"
	@ rm -fr html
	@ rm -fr headers
	
