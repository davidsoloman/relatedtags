	/*
 */
package com.ali.interview

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class SparkExampleSpec extends FlatSpec with SparkRelatedtagsSpec with GivenWhenThen with Matchers {

  "Empty set" should "tested" in {
    Given("empty set")
    val lines = Array("")

    When("related tags")
    val bigramString = lines

    Then("empty tags")
    bigramString shouldBe empty
  }

  "Related tags" should "be paired" in {
    Given("tags")
    val lines = Array("A,B,C", "A,Lemon")

    When("related tags")
    val bigramString = lines

    Then("related tags")
    bigramString shouldBe lines
  }

}
