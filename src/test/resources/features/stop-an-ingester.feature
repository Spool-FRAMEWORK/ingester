Feature: A stopped ingester leaves the bus

  An ingester listens on the bus for the envelopes stored in the inbox. Once stopped, it no longer takes any of them,
  so the ingesters that are still running write every envelope to the data lake.

  @DEV-123
  Scenario: An envelope announced after an ingester stops is written by the ingester that is running
    Given two ingesters listening on the same bus
    And the first ingester is stopped
    And the envelope "order-1" is captured in the inbox
    When the bus announces that "order-1" was stored
    Then the second ingester writes "order-1" to the data lake
    And the first ingester writes nothing to the data lake

  @DEV-123
  Scenario: An ingester started again after stopping writes each envelope once
    Given an ingester listening on the bus
    And the ingester is stopped
    And the ingester is started again
    And the envelope "order-2" is captured in the inbox
    When the bus announces that "order-2" was stored
    Then the ingester writes "order-2" to the data lake once
