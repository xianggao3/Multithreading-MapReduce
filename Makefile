CC := gcc
PROF := gprof
GOOGPROF := google-pprof
SRCD := src
BLDD := build
BIND := bin
INCD := include

_SRCF := $(shell find $(SRCD) -type f -name *.c)
_OBJF := $(patsubst $(SRCD)/%,$(BLDD)/%,$(_SRCF:.c=.o))
INC := -I $(INCD)
PROFFLAG :=

EXEC := lott

CFLAGS := -Wall -Werror -std=gnu11
DFLAGS := -g -DDEBUG
LIBS := -lpthread
PROFLIB := -Wl,--no-as-needed,-lprofiler,--as-needed


.PHONY: clean all

all: setup $(EXEC)

profile: CFLAGS += $(PROFFLAG)
profile: all

debug: CFLAGS += $(DFLAGS)
debug: all

setup:
	mkdir -p bin build

$(EXEC): $(_OBJF)
	$(CC) $(CFLAGS) $^ -o $(BIND)/$@ $(LIBS)

$(BLDD)/%.o: $(SRCD)/%.c
	$(CC) $(CFLAGS) $(INC) -c -o $@ $<

clean:
	$(RM) -r $(BLDD) $(BIND)
