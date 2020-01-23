package cat.altimiras.shepherd.monitoring;

public enum Stats {
	ElapsedTimeCollectingMs, //milliseconds collecting stats
	NumElements, //groups pending to group
	NumElementsTodal, //total elements inside groups
	AvgElementsGroup, //avg elements inside groups  NUM_ELEMENTS_TOTAL/NUM_ELEMENTS
	MaxElementGroup, //num of elements of the group with max number of elements
	MinElementGroup, //num of elements of the group with max number of elements
	OldestElement, //oldest element
	AgeOldestElementS, // seconds in the system

	Elements //content elements,
}


