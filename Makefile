ifdef DEBUG
ERLC_FLAGS=-I ../cassandra/include +debug_info
else
ERLC_FLAGS=-I ../cassandra/include
endif
SOURCES=$(wildcard src/*.erl)
EBIN_DIR=ebin
SRC_DIR=src
OBJECTS=$(SOURCES:$(SRC_DIR)/%.erl=$(EBIN_DIR)/%.beam)

.PHONY: all
all: $(OBJECTS)

$(EBIN_DIR)/%.beam: $(SRC_DIR)/%.erl
	erlc $(ERLC_FLAGS) -o $(EBIN_DIR) $<

.PHONY: clean
clean:
	@-rm -f $(OBJECTS)
