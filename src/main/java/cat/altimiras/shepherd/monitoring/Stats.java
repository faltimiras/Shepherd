package cat.altimiras.shepherd.monitoring;

public enum Stats {
	ELAPSED_TIME_COLLECTING_ms, //milliseconds collecting stats
	NUM_ELEMENTS, //groups pending to group
	NUM_ELEMENTS_TOTAL, //total elements inside groups
	AVG_ELEMENTS_GROUP, //avg elements inside groups  NUM_ELEMENTS_TOTAL/NUM_ELEMENTS
	MAX_ELEMENT_GROUP, //num of elements of the group with max number of elements
	MIN_ELEMENT_GROUP, //num of elements of the group with max number of elements
	OLDEST_ELEMENT, //oldest element
	AGE_OLDEST_ELEMENT_s // seconds in the system
}


