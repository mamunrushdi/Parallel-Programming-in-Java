package edu.coursera.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
 

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

    /**
     * Default constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Sequentially compute the sum of the reciprocal values for a given array.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // Compute sum of reciprocals of array elements
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the start of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     *         nElements
     */
    private static int getChunkStartInclusive(final int chunk,
            final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the end of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {
      
    	static final int SEQUENTIAL_THREASHOLD = 50000;
    	/**
         * Starting index for traversal done by this task.
         */
        private final int startIndexInclusive;
        /**
         * Ending index for traversal done by this task.
         */
        private final int endIndexExclusive;
        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;
        /**
         * Intermediate value produced by this task.
         */
        private double value;

        /**
         * Constructor.
         * @param setStartIndexInclusive Set the starting index to begin
         *        parallel traversal at.
         * @param setEndIndexExclusive Set ending index for parallel traversal.
         * @param setInput Input values
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                final int setEndIndexExclusive, final double[] setInput) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
        }

        /**
         * Getter for the value produced by this task.
         * @return Value produced by this task
         */
        public double getValue() {
            return value;
        }

        @Override
        protected void compute() {
            // TODO
        	for(int i = startIndexInclusive; i < endIndexExclusive; i++)
        	{
        		value += 1/input[i];
        	}
//            System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism","4");
//
//         	if(startIndexInclusive > endIndexExclusive) value = 0;
//			else if( endIndexExclusive - startIndexInclusive <= SEQUENTIAL_THREASHOLD)
//			{				
//				for(int i = 0; i < input.length; i++)
//					value += 1/input[i]; 
//			}
//			else
//			{
//				/*
//				 * divide the array to receive the range difference to THRESHOLD
//				 */
//				//get the mid index of the given array
//				int midIndex = input.length /2;
//				//System.out.println("mid index: " + midIndex);
//				double[] leftSubArray = new double[midIndex];
//				for(int i = 0; i < midIndex; i++)
//				{
//					//System.out.println("leftSubArray[i]: " + arr[i]);
//					leftSubArray[i] = input[i];
//				} 
//				
//				double[] rightSubArray = new double[input.length - midIndex];
//				//System.out.println("size of rightSubArray: " + rightSubArray.length);
//				for(int i = 0; i < rightSubArray.length; i++)
//				{
//					//System.out.println("rightSubArray[i]]: " + arr[midIndex + i]);
//					rightSubArray[i] = input[midIndex + i];
//				} 
//				
//				//create a new Task with different lower and higher range
//				ReciprocalArraySumTask leftTask = new ReciprocalArraySumTask(0, midIndex, leftSubArray);
//				ReciprocalArraySumTask rightTask = new ReciprocalArraySumTask(midIndex, rightSubArray.length, rightSubArray);
//				
//				 
//				//compute the task in parallel
//				leftTask.fork();
//				rightTask.compute();
//				leftTask.join(); 
//				value = leftTask.getValue() +rightTask.getValue();
//			} 
        }
    }

    /**
     * TODO: Modify this method to compute the same reciprocal sum as
     * seqArraySum, but use two tasks running in parallel under the Java Fork
     * Join framework. You may assume that the length of the input array is
     * evenly divisible by 2.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double parArraySum(final double[] input) {
        assert input.length % 2 == 0;
        
        int midIndex = input.length/2;
        
        ReciprocalArraySumTask leftHalfTask = new ReciprocalArraySumTask(0, midIndex , input);
        ReciprocalArraySumTask rightHalfTask = new ReciprocalArraySumTask(midIndex, input.length , input);
        ForkJoinTask.invokeAll(leftHalfTask, rightHalfTask);
       
//        //calculate the left half
//        leftHalfTask.fork();
//        
//        //calculate the right half task
//        rightHalfTask.compute();
//        
//        //complete the left half result
//        leftHalfTask.join();
        
       // ForkJoinTask.invokeAll(leftHalfTask, rightHalfTask);
        //combine the result
         return leftHalfTask.getValue() + rightHalfTask.getValue();
        
    }

    /**
     * TODO: Extend the work you did to implement parArraySum to use a set
     * number of tasks to compute the reciprocal array sum. You may find the
     * above utilities getChunkStartInclusive and getChunkEndExclusive helpful
     * in computing the range of element indices that belong to each chunk.
     *
     * @param input Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum(final double[] input,
            final int numTasks) {
        double sum = 0;

        // Compute sum of reciprocals of array elements
        
        //create numTask amount of task
        List<ReciprocalArraySumTask> subTasksList = createSubTask(input, numTasks);  
        
        RecursiveAction.invokeAll(subTasksList);
        
        for(ReciprocalArraySumTask subTask : subTasksList)
        {
        	subTask.join();
        	sum += subTask.getValue();
        }
        return sum;
    }
    
    private static List<ReciprocalArraySumTask> createSubTask(double[] input, int numTasks)
    {
    	  int taskNum = numTasks;
    	  
         if(taskNum>input.length)
         {
             taskNum = input.length;
         }
    	List<ReciprocalArraySumTask> subTaskList = new ArrayList<>();
    	for(int i = 0; i < numTasks; i++)
    	{ 
    		ReciprocalArraySumTask subtask = new ReciprocalArraySumTask(getChunkStartInclusive(i, taskNum, input.length), getChunkEndExclusive(i, taskNum, input.length), input);
    		subTaskList.add(subtask);
    	}
    	return subTaskList;
    }
}
