package org.project.elasticsearchSink;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;

import java.io.Serializable;
import java.util.Objects;

/** A single stream element which contains a BulkOperationVariant. */
public class Operation implements Serializable {
    private static final long serialVersionUID = 1L;

    private final BulkOperationVariant bulkOperationVariant;

    public Operation(BulkOperationVariant bulkOperation) {
        this.bulkOperationVariant = bulkOperation;
    }

    public BulkOperationVariant getBulkOperationVariant() {
        return bulkOperationVariant;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bulkOperationVariant);
    }

    @Override
    public String toString() {
        return "Operation{" + "bulkOperationVariant=" + bulkOperationVariant + '}';
    }
}