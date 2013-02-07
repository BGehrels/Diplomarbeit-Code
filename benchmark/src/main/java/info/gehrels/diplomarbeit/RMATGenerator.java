package info.gehrels.diplomarbeit;

import info.gehrels.diplomarbeit.GraphGenerationBatch.EdgeWriterFactory;
import info.gehrels.diplomarbeit.GraphGenerationBatch.NodeWriter;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class RMATGenerator implements Generator {
	private static final Random RANDOM = new Random();

	public static final int TOP_LEFT = 0;
	public static final int TOP_RIGHT = 1;
	public static final int BOTTOM_LEFT = 2;
	public static final int BOTTOM_RIGHT = 3;

	private static final byte TOP_LEFT_PERCENTAGE = 58;
	private static final byte TOP_RIGHT_PERCENTAGE = 19;
	private static final byte BOTTOM_LEFT_PERCENTAGE = 19;

	private final EdgeWriterFactory edgeWriterFactory;
	private final int numberOfNodes;
	private final long expectedNumberOfEdges;
	final Set<String> adjacencyMatrix = new HashSet<>();
	int actualNumberOfEdges;

	public RMATGenerator(EdgeWriterFactory edgeWriterFactory, NodeWriter nodeWriter, int numberOfNodes,
	                     long expectedNumberOfEdges) {
		this.edgeWriterFactory = edgeWriterFactory;
		this.numberOfNodes = numberOfNodes;
		this.expectedNumberOfEdges = expectedNumberOfEdges;
		for (int i = 0; i < numberOfNodes; i++) {
			nodeWriter.createNode("" + i);
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
			if (!hasEdge(topLeftX, topLeftY)) {
				actualNumberOfEdges++;
				markEdge(topLeftX, topLeftY);
				edgeWriterFactory.createEdge("" + topLeftX, "" + topLeftY);
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

	private void markEdge(int topLeftX, int topLeftY) {
		adjacencyMatrix.add(topLeftX + ";" + topLeftY);
	}

	private boolean hasEdge(int topLeftX, int topLeftY) {
		return adjacencyMatrix.contains(topLeftX + ";" + topLeftY);
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
