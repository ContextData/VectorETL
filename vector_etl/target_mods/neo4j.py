import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, ClientError
from .base import BaseTarget

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Neo4jTarget(BaseTarget):
    def __init__(self, config):
        self.config = config
        self.driver = None

    def connect(self):
        logger.info("Connecting to Neo4j...")
        try:
            self.driver = GraphDatabase.driver(
                self.config['neo4j_uri'],
                auth=(self.config['username'], self.config['password'])
            )
            with self.driver.session() as session:
                session.run("RETURN 1")
            logger.info("Connected to Neo4j successfully.")
        except ServiceUnavailable as e:
            logger.error(f"Failed to connect to Neo4j: {str(e)}")
            raise

    def sanitize_property_name(self, name):
        return name.replace(' ', '_').replace('-', '_')

    def create_index_if_not_exists(self):
        if self.driver is None:
            self.connect()

        with self.driver.session() as session:
            # Create index on Entity node
            try:
                session.run("CREATE INDEX IF NOT EXISTS FOR (e:Entity) ON (e.id)")
                logger.info("Created index on Entity.id")
            except ClientError as e:
                logger.warning(f"Failed to create index on Entity.id: {str(e)}")

            for node in self.config['graph_structure']['nodes']:
                label = node['label']
                for prop in node['properties']:
                    sanitized_prop = self.sanitize_property_name(prop)
                    try:
                        session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.{sanitized_prop})")
                        logger.info(f"Created index on {label}.{sanitized_prop}")
                    except ClientError as e:
                        logger.warning(f"Failed to create index on {label}.{sanitized_prop}: {str(e)}")

            # Create vector index on Entity node
            vector_prop = self.sanitize_property_name(self.config['vector_property'])
            try:
                session.run(f"""
                    CREATE VECTOR INDEX {vector_prop}_index IF NOT EXISTS
                    FOR (e:Entity) ON (e.{vector_prop})
                    OPTIONS {{
                        indexConfig: {{
                            `vector.dimensions`: {self.config['vector_dimensions']},
                            `vector.similarity_function`: '{self.config['similarity_function']}'
                        }}
                    }}
                """)
                logger.info(f"Created vector index on Entity.{vector_prop}")
            except ClientError as e:
                logger.warning(f"Failed to create vector index: {str(e)}. This may be due to Neo4j version limitations.")

    def build_cypher_query(self):
        nodes = self.config['graph_structure']['nodes']
        relationships = self.config['graph_structure']['relationships']

        # Create Entity node
        vector_prop = self.sanitize_property_name(self.config['vector_property'])
        create_entity = f"CREATE (e:Entity {{id: row.id, {vector_prop}: row.embedding}})"

        # Create specific nodes and connect to Entity
        create_nodes = []
        for node in nodes:
            props = ", ".join([f"{self.sanitize_property_name(prop)}: row.metadata['{prop}']" for prop in node['properties']])
            create_nodes.append(f"""
                CREATE (n_{node['label']}:{node['label']} {{{props}}})
                CREATE (e)-[:HAS_{node['label']}]->(n_{node['label']})
            """)

        # Create relationships between specific nodes
        create_relationships = []
        for rel in relationships:
            create_relationships.append(f"CREATE (n_{rel['start_node']})-[:{rel['type']}]->(n_{rel['end_node']})")

        # Combine all parts
        cypher_query = f"""
        UNWIND $batch AS row
        {create_entity}
        {" ".join(create_nodes)}
        {" ".join(create_relationships)}
        """

        return cypher_query

    def write_data(self, df, columns, domain=None):
        logger.info("Writing data to Neo4j...")
        if self.driver is None:
            self.connect()

        self.create_index_if_not_exists()

        def create_graph(tx, batch):
            query = self.build_cypher_query()
            tx.run(query, batch=batch)

        batch_size = 1000  # Adjust based on your needs
        total_processed = 0

        with self.driver.session() as session:
            for i in range(0, len(df), batch_size):
                batch = []
                for _, row in df.iloc[i:i+batch_size].iterrows():
                    node = {
                        'id': str(row['df_uuid']),
                        'embedding': row['embeddings'],
                        'metadata': {k: v for k, v in row.items() if k not in ['df_uuid', 'embeddings']}
                    }
                    if domain:
                        node['metadata']['domain'] = domain
                    batch.append(node)

                session.execute_write(create_graph, batch)
                total_processed += len(batch)
                logger.info(f"Processed {total_processed} out of {len(df)} records.")

        logger.info("Completed writing data to Neo4j.")

    def close(self):
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed.")

    def __del__(self):
        self.close()
