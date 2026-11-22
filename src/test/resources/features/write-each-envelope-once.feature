Feature: The ingester writes each envelope once while it remembers its key

  An envelope can be announced more than once: the bus delivers at least once, and the janitor republishes an
  envelope that looks stuck while it only waits in a batch. The ingester remembers the keys it wrote during a
  window. A repeated envelope is not written again, but its EnvelopePersisted is published again so that nobody
  waits for it. A rejected envelope is not remembered (ADR-007, ADR-010).

  Background:
    Given an ingester that remembers the keys it wrote for 10 minutes

  @DEV-64
  Scenario: An envelope announced again after its batch was written is not written again
    Given the envelope "order-1" is captured in the inbox
    When the ingester is told that "order-1" was stored
    And the ingester writes its batch
    And the ingester is told that "order-1" was stored
    And the ingester writes its batch
    Then "order-1" was written to the data lake once
    And EnvelopePersisted was published 2 times for "order-1"

  @DEV-64
  Scenario: An envelope announced again while it waits in the batch is written once
    Given the envelope "order-2" is captured in the inbox
    When the ingester is told that "order-2" was stored
    And the ingester is told that "order-2" was stored
    And the ingester writes its batch
    Then "order-2" was written to the data lake once
    And EnvelopePersisted was published 1 time for "order-2"

  @DEV-64
  Scenario: A rejected envelope is validated again when it arrives again
    Given the envelope "order-3" is captured in the inbox with a payload that fails validation
    When the ingester is told that "order-3" was stored
    And the ingester writes its batch
    And the ingester is told that "order-3" was stored
    And the ingester writes its batch
    Then "order-3" was sent to quarantine 2 times
    And "order-3" was not written to the data lake

  @DEV-64
  Scenario: After the window, the same envelope is written as a new one
    Given the envelope "order-4" is captured in the inbox
    When the ingester is told that "order-4" was stored
    And the ingester writes its batch
    And 11 minutes pass
    And the ingester is told that "order-4" was stored
    And the ingester writes its batch
    Then "order-4" was written to the data lake 2 times
