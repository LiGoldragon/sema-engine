use std::{collections::BTreeMap, path::PathBuf};

use core_ethos::bootstrap::{
    BootstrapCatalog, BootstrapGrammarIdentities, BootstrapPriorIdentities,
    BootstrapPriorVocabulary, BootstrapVersionPolicy, CanonicalIdentityOrder, EthosVersion,
    IdentitySchema, IdentitySchemaCatalog, InterfaceRole, NomosSchema, SchemaRole,
    TextualMetadataRecord, TextualMetadataSnapshot, TextualProjectionAddress,
};
use core_nomos::{ExternalStorageProvenance, StorageProvenanceOwner};
use name_table::{LocalEncodedId, Name};
use rust_logos::{
    FixtureRustVocabulary, FixtureRustVocabularyIds, RustEncodedIdCodec, RustLogos, RustTypePath,
    RustTypePathResolver,
};
use schema_rust::bootstrap::{BootstrapSemaGeneration, GeneratedBootstrapSema};
use sema_translator::bootstrap::{
    AuthorizedBootstrapTransition, BootstrapAuthorityIdentity, BootstrapAuthorityRevision,
    BootstrapTransactionAssembler,
};
use signal_sema_translator::{VocabularyEncodedId, VocabularyRoot};
use structural_codec::EncodedNameResolver;

pub const UPDATE_VARIABLE: &str = "UPDATE_SEMA_ENGINE_GENERATED_TABLE";

pub fn generated_sema_table() -> GeneratedBootstrapSema {
    let catalog = catalog();
    let approval = approval(&catalog);
    let assembly = BootstrapTransactionAssembler::new(
        BootstrapAuthorityIdentity::new([0x68; 32]),
        BootstrapAuthorityRevision::new(1),
        BootstrapGrammarIdentities {
            document: universal_id(900),
            syntax: universal_id(901),
        },
        catalog,
    )
    .assemble(include_str!("../../schema/witness.sema"), approval)
    .expect("checked Sema source is authority-approved");
    let rust = rust_logos();
    let paths = TypePaths::default()
        .with(universal_id(7), &["std", "string", "String"])
        .with(universal_id(8), &["u64"]);
    let external_storage = [external_storage(7, 7), external_storage(8, 8)];
    BootstrapSemaGeneration::new(
        &assembly,
        &rust,
        &paths,
        &external_storage,
        source_path(),
        generated_rust_path(),
    )
    .generate()
    .expect("authority-verified Sema lowers to the checked Rust projection")
}

fn source_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("schema/witness.sema")
}

fn generated_rust_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/generated_sema_table.rs")
}

fn universal_id(local: u16) -> VocabularyEncodedId {
    VocabularyEncodedId::new(VocabularyRoot::Universal, vec![LocalEncodedId::new(local)])
        .expect("fixture authority identity is nonempty")
}

fn rust_id(local: u16) -> VocabularyEncodedId {
    VocabularyEncodedId::new(VocabularyRoot::Rust, vec![LocalEncodedId::new(local)])
        .expect("fixture Rust identity is nonempty")
}

fn metadata_record(spelling: &str, identity: VocabularyEncodedId) -> TextualMetadataRecord {
    TextualMetadataRecord {
        address: TextualProjectionAddress {
            module_path: vec!["sema_engine".to_owned()],
            lexical_owner: None,
            visible_name: spelling.to_owned(),
        },
        encoded_name: identity,
    }
}

fn prior_identities() -> BootstrapPriorIdentities {
    BootstrapPriorIdentities {
        interface_kind: universal_id(1),
        nexus_kind: universal_id(2),
        sema_kind: universal_id(3),
        input_role: universal_id(4),
        output_role: universal_id(5),
        refusal_role: universal_id(6),
        string_type: universal_id(7),
        integer_type: universal_id(8),
        boolean_type: universal_id(9),
        unit_type: universal_id(10),
        vector_shape: universal_id(11),
        option_shape: universal_id(12),
        map_shape: universal_id(13),
        result_shape: universal_id(14),
        stream_nomos: universal_id(15),
        stream_shape: universal_id(15),
        stream_identity_shape: universal_id(16),
    }
}

fn catalog() -> BootstrapCatalog {
    let specifications = [
        (
            1,
            "Interface",
            vec![SchemaRole::FileKind(
                core_ethos::bootstrap::EthosKind::Interface,
            )],
        ),
        (
            2,
            "Nexus",
            vec![SchemaRole::FileKind(
                core_ethos::bootstrap::EthosKind::Nexus,
            )],
        ),
        (
            3,
            "Sema",
            vec![SchemaRole::FileKind(core_ethos::bootstrap::EthosKind::Sema)],
        ),
        (
            4,
            "Input",
            vec![SchemaRole::InterfaceRole(InterfaceRole::Input)],
        ),
        (
            5,
            "Output",
            vec![SchemaRole::InterfaceRole(InterfaceRole::Output)],
        ),
        (
            6,
            "Refusal",
            vec![SchemaRole::InterfaceRole(InterfaceRole::Refusal)],
        ),
        (7, "String", vec![SchemaRole::Nominal { persistent: true }]),
        (8, "Integer", vec![SchemaRole::Nominal { persistent: true }]),
        (9, "Boolean", vec![SchemaRole::Nominal { persistent: true }]),
        (10, "Unit", vec![SchemaRole::Nominal { persistent: true }]),
        (11, "Vector", vec![SchemaRole::Shape { arity: 1 }]),
        (12, "Option", vec![SchemaRole::Shape { arity: 1 }]),
        (13, "Map", vec![SchemaRole::Shape { arity: 2 }]),
        (14, "Result", vec![SchemaRole::Shape { arity: 2 }]),
        (
            15,
            "Stream",
            vec![
                SchemaRole::Shape { arity: 1 },
                SchemaRole::Nomos(NomosSchema::StreamInitiation { arity: 2 }),
            ],
        ),
        (16, "StreamIdentity", vec![SchemaRole::Shape { arity: 1 }]),
    ];
    let metadata = TextualMetadataSnapshot::new(
        specifications
            .iter()
            .map(|(local, spelling, _)| metadata_record(spelling, universal_id(*local)))
            .collect(),
    )
    .expect("prior metadata is exact");
    let schemas = IdentitySchemaCatalog::new(
        specifications
            .iter()
            .map(|(local, _, roles)| {
                IdentitySchema::new(universal_id(*local), roles.clone())
                    .expect("prior schema is valid")
            })
            .collect(),
    )
    .expect("prior schemas are exact");
    let priors = BootstrapPriorVocabulary::new(prior_identities(), &schemas, &metadata)
        .expect("prior relationships are valid");
    let order = CanonicalIdentityOrder::new(
        specifications
            .iter()
            .map(|(local, _, _)| (universal_id(*local), vec![0x10, *local as u8])),
    )
    .expect("prior order is unique");
    BootstrapCatalog::new(
        vec!["sema_engine".to_owned()],
        metadata,
        schemas,
        priors,
        BootstrapVersionPolicy::exact(EthosVersion::new(1, 0, 0)),
        order,
    )
    .expect("bootstrap catalog is coherent")
}

fn approval(catalog: &BootstrapCatalog) -> AuthorizedBootstrapTransition {
    let declarations = [
        ("Domain", universal_id(100)),
        ("DomainSnapshot", universal_id(101)),
        ("domain_snapshots", universal_id(102)),
    ];
    let mut after = catalog.metadata().records().to_vec();
    after.extend(
        declarations
            .iter()
            .map(|(spelling, identity)| metadata_record(spelling, identity.clone())),
    );
    AuthorizedBootstrapTransition::new(
        TextualMetadataSnapshot::new(after).expect("approved metadata is exact"),
        declarations
            .iter()
            .map(|(_, identity)| {
                let local = identity.chain()[0].value();
                (
                    identity.clone(),
                    vec![0x80, (local >> 8) as u8, local as u8],
                )
            })
            .collect(),
        BTreeMap::new(),
    )
}

fn external_storage(local: u16, fingerprint: u8) -> ExternalStorageProvenance {
    ExternalStorageProvenance::new(
        universal_id(local),
        [fingerprint; 32],
        StorageProvenanceOwner::new(
            "https://github.com/LiGoldragon/core-ethos".to_owned(),
            "7a1384874f3747de97c6ccbb4ae6fa2149b27330".to_owned(),
        )
        .expect("storage provenance names an exact owner revision"),
    )
    .expect("external storage identity is Universal")
}

#[derive(Default)]
struct Names(BTreeMap<VocabularyEncodedId, Name>);

impl EncodedNameResolver<VocabularyRoot> for Names {
    fn resolve(&self, encoded_id: &VocabularyEncodedId) -> Option<&Name> {
        self.0.get(encoded_id)
    }
}

fn rust_logos() -> RustLogos {
    let ids = FixtureRustVocabularyIds::new(
        rust_id(10),
        rust_id(11),
        rust_id(12),
        rust_id(13),
        rust_id(14),
        rust_id(1),
        rust_id(2),
        rust_id(3),
        rust_id(4),
        rust_id(5),
    );
    let mut names = Names::default();
    for (identity, spelling) in [
        (rust_id(10), "NewtypeItemRecord"),
        (rust_id(11), "EnumerationItemRecord"),
        (rust_id(12), "VariantRecord"),
        (rust_id(13), "TupleFieldRecord"),
        (rust_id(14), "TypeReferenceRecord"),
        (rust_id(1), "struct"),
        (rust_id(2), "enum"),
        (rust_id(3), "pub"),
        (rust_id(4), ","),
        (rust_id(5), ";"),
    ] {
        names.0.insert(identity, Name::new(spelling));
    }
    RustLogos::new(
        FixtureRustVocabulary::seal(ids, &names).expect("caller-owned Rust vocabulary is sealed"),
    )
}

#[derive(Default)]
struct TypePaths(BTreeMap<VocabularyEncodedId, RustTypePath>);

impl TypePaths {
    fn with(mut self, identity: VocabularyEncodedId, segments: &[&str]) -> Self {
        self.0.insert(
            identity,
            RustTypePath::try_new(
                segments
                    .iter()
                    .map(|segment| (*segment).to_owned())
                    .collect(),
            )
            .expect("explicit Rust type path is valid"),
        );
        self
    }
}

impl RustTypePathResolver for TypePaths {
    fn resolve_type_path(&self, encoded_id: &VocabularyEncodedId) -> Option<&RustTypePath> {
        self.0.get(encoded_id)
    }
}

pub fn generated_identity(local: u16) -> String {
    RustEncodedIdCodec::encode(&universal_id(local))
}
