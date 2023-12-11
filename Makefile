JAVAC=javac
SOURCE=MyDedup.java
CLASS=MyDedup

all: compile

compile:
	$(JAVAC) $(SOURCE)

clean:
	rm -f *.class