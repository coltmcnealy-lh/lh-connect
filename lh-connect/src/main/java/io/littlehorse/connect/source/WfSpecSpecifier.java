package io.littlehorse.connect.source;

import java.util.Optional;

public class WfSpecSpecifier {

    private final String name;
    private final Integer majorVersion;
    private final Integer revision;

    public WfSpecSpecifier(String name, int majorVersion, int revision) {
        this.name = name;
        this.majorVersion = majorVersion;
        this.revision = revision;
    }

    public String getWfSpecName() {
        return name;
    }

    public Optional<Integer> getMajorVersion() {
        return Optional.ofNullable(majorVersion);
    }

    public Optional<Integer> getRevision() {
        return Optional.ofNullable(revision);
    }
}
