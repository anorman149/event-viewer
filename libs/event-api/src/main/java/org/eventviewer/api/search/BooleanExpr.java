package org.eventviewer.api.search;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public final class BooleanExpr implements Expression {

    private final List<Expression> must;
    private final List<Expression> should;
    private final List<Expression> mustNot;

    @JsonCreator
    private BooleanExpr(
            @JsonProperty("must") List<Expression> must,
            @JsonProperty("should") List<Expression> should,
            @JsonProperty("mustNot") List<Expression> mustNot) {
        this.must = Collections.unmodifiableList(must != null ? must : new ArrayList<>());
        this.should = Collections.unmodifiableList(should != null ? should : new ArrayList<>());
        this.mustNot = Collections.unmodifiableList(mustNot != null ? mustNot : new ArrayList<>());
    }

    public static BooleanExpr must(Expression... expressions) {
        return new BooleanExpr(Arrays.asList(expressions), List.of(), List.of());
    }

    public static BooleanExpr should(Expression... expressions) {
        return new BooleanExpr(List.of(), Arrays.asList(expressions), List.of());
    }

    public static BooleanExpr mustNot(Expression... expressions) {
        return new BooleanExpr(List.of(), List.of(), Arrays.asList(expressions));
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<Expression> must() { return must; }
    public List<Expression> should() { return should; }
    public List<Expression> mustNot() { return mustNot; }

    public static final class Builder {
        private final List<Expression> must = new ArrayList<>();
        private final List<Expression> should = new ArrayList<>();
        private final List<Expression> mustNot = new ArrayList<>();

        public Builder must(Expression... expressions) {
            must.addAll(Arrays.asList(expressions));
            return this;
        }

        public Builder should(Expression... expressions) {
            should.addAll(Arrays.asList(expressions));
            return this;
        }

        public Builder mustNot(Expression... expressions) {
            mustNot.addAll(Arrays.asList(expressions));
            return this;
        }

        public BooleanExpr build() {
            return new BooleanExpr(new ArrayList<>(must), new ArrayList<>(should), new ArrayList<>(mustNot));
        }
    }
}
