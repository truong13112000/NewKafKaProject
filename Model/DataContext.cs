
using Microsoft.EntityFrameworkCore;

namespace ApacheKafkaProducerDemo.Model
{
    public class DataContext : DbContext
    {
        public DataContext(DbContextOptions<DataContext> options) : base(options) { }
        public DbSet<KafkaEntities> KafkaEntities { get; set; }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            //modelBuilder.Entity<KafkaEntities>().HasNoKey();
        }
    }
}
