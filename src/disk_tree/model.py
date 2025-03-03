from tortoise import models, Tortoise
from tortoise.fields import IntField, CharField, DatetimeField, BigIntField, ForeignKeyField


class FileEntry(models.Model):
    path = CharField(max_length=1024, pk=True)
    mtime = DatetimeField()
    size = BigIntField()
    parent = ForeignKeyField('disk_tree.FileEntry', related_name='children', to_field='path', null=True, index=True)
    kind = CharField(max_length=4)  # 'file' or 'dir'
    num_descendants = IntField()

    def children(self):
        return FileEntry.filter(parent=self.path)

    def __str__(self):
        return f"{self.kind}: {self.path} ({self.size} bytes)"

    class Meta:
        table = "file"


async def init_db(db_path: str = "disk_tree.db"):
    """Initialize the database connection"""
    await Tortoise.init(
        db_url=f"sqlite://{db_path}",
        modules={"disk_tree": ["disk_tree.model"]}
    )
    # Create tables
    await Tortoise.generate_schemas()
