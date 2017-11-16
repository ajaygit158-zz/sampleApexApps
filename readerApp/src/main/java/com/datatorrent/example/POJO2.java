package com.datatorrent.example;

/**
 * Created by bhupesh on 6/11/17.
 */
public class POJO2
{
  private int random;

  public POJO2()
  {
  }

  public POJO2(int num)
  {
    this.random = num;
  }


  public int getRandom()
  {
    return random;
  }

  public void setRandom(int random)
  {
    this.random = random;
  }

  @Override
  public String toString()
  {
    return "POJO{" +
      ", random=" + random +
      '}';
  }
}
