package info.gehrels.diplomarbeit.neo4j;

import info.gehrels.diplomarbeit.neo4j.GraphGenerationBatch.MyEdgeFactory;
import info.gehrels.diplomarbeit.neo4j.GraphGenerationBatch.MyNodeFactory;

import java.util.Random;

public class RMATGenerator implements Generator {
	private static final Random RANDOM = new Random();

	public static final int TOP_LEFT = 0;
	public static final int TOP_RIGHT = 1;
	public static final int BOTTOM_LEFT = 2;
	public static final int BOTTOM_RIGHT = 3;

	private static final byte TOP_LEFT_PERCENTAGE = 58;
	private static final byte DIVISOR = 3;
	private static final byte TOP_RIGHT_PERCENTAGE = TOP_LEFT_PERCENTAGE / DIVISOR;
	private static final byte BOTTOM_LEFT_PERCENTAGE = TOP_LEFT_PERCENTAGE / DIVISOR;

	private final MyEdgeFactory edgeFactory;
	private final int numberOfNodes;
	private final int expectedNumberOfEdges;
	final boolean[][] adjacencyMatrix;
	int actualNumberOfEdges;

	public RMATGenerator(MyEdgeFactory edgeFactory, MyNodeFactory nodeFactory, int numberOfNodes,
	                     int expectedNumberOfEdges) {
		this.edgeFactory = edgeFactory;
		this.numberOfNodes = numberOfNodes;
		this.expectedNumberOfEdges = expectedNumberOfEdges;
		this.adjacencyMatrix = new boolean[numberOfNodes][numberOfNodes];
		for (int i = 0; i < numberOfNodes; i++) {
			nodeFactory.createNode("" + i);
		}
	}

	@Override
	public void generate() {
		while (actualNumberOfEdges < expectedNumberOfEdges) {
			createEdge(0, 0, numberOfNodes - 1, numberOfNodes - 1);
		}
	}

	private void createEdge(int topLeftX, int topLeftY, int bottomRightX, int bottomRightY) {
		int fieldSize = bottomRightX - topLeftX + 1;

		if (fieldSize == 1) {
			if (!adjacencyMatrix[topLeftX][topLeftY]) {
				actualNumberOfEdges++;
				adjacencyMatrix[topLeftX][topLeftY] = true;
				edgeFactory.createEdge("" + topLeftX, "" + topLeftY);
			}
		} else {
			int quadrant = throwTheQuadrantDice();
			switch (quadrant) {
				case TOP_LEFT:
					createEdge(topLeftX, topLeftY, bottomRightX - fieldSize / 2, bottomRightY - fieldSize / 2);
					break;
				case TOP_RIGHT:
					createEdge(topLeftX + fieldSize / 2, topLeftY, bottomRightX, bottomRightY - fieldSize / 2);
					break;
				case BOTTOM_LEFT:
					createEdge(topLeftX, topLeftY + fieldSize / 2, bottomRightX - fieldSize / 2, bottomRightY);
					break;
				case BOTTOM_RIGHT:
					createEdge(topLeftX + fieldSize / 2, topLeftY + fieldSize / 2, bottomRightX, bottomRightY);
					break;
			}
		}
	}

	private int throwTheQuadrantDice() {
		int rndVal = RANDOM.nextInt(100);
		if (rndVal < TOP_LEFT_PERCENTAGE) {
			return TOP_LEFT;
		} else if (rndVal < TOP_LEFT_PERCENTAGE + TOP_RIGHT_PERCENTAGE) {
			return TOP_RIGHT;
		} else if (rndVal < TOP_LEFT_PERCENTAGE + TOP_RIGHT_PERCENTAGE + BOTTOM_LEFT_PERCENTAGE) {
			return BOTTOM_LEFT;
		} else {
			return BOTTOM_RIGHT;
		}
	}
}
