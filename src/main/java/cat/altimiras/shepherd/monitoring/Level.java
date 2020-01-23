package cat.altimiras.shepherd.monitoring;

public enum Level {
	INFO(0),
	DEBUG(1),
	TRACE(2);

	int level;

	Level(int level) {
		this.level = level;
	}

	public int getLevel() {
		return level;
	}
}
